import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.charset.StandardCharsets
import scala.collection.mutable

object Main {

  // Comprime latitudine e longitudine in un'unico numero intero per risparmiare allocazioni di memoria (16 bit ciascuno)
  def packCell(latTenths: Int, lonTenths: Int): Int =
    (latTenths << 16) | (lonTenths & 0xFFFF)

  def unpackLat(cell: Int): Int = cell >> 16
  def unpackLon(cell: Int): Int = (cell << 16) >> 16

  // Fa l'unpack di latitudine e longitudine e reinserisce la virgola al punto giusto
  def formatCell(cell: Int): String =
    String.format(java.util.Locale.US, "(%.1f, %.1f)",
      java.lang.Double.valueOf(unpackLat(cell) / 10.0),
      java.lang.Double.valueOf(unpackLon(cell) / 10.0))

  // Arrotondamento
  def toTenths(raw: String): Int =
    new java.math.BigDecimal(raw.trim)
      .movePointRight(1)
      .setScale(0, java.math.RoundingMode.HALF_UP)
      .intValueExact()

  // Converte la data
  def dateToInt(raw: String): Int =
    raw.trim.substring(0, 10).replace("-", "").toInt

  def dateIntToString(d: Int): String = {
    val s = d.toString
    s"${s.substring(0, 4)}-${s.substring(4, 6)}-${s.substring(6, 8)}"
  }

  // Legge le colonne dalla riga del CSV, le converte con le funzioni sopra, e ritorna (data, cella) come coppia di Int.
  def normalizeEvent(row: Row): (Int, Int) = {
    try {
      val lat = toTenths(row.getAs[String]("latitude"))
      val lon = toTenths(row.getAs[String]("longitude"))
      val date = dateToInt(row.getAs[String]("date"))
      (date, packCell(lat, lon))
    } catch {
      case _: Exception => (Int.MinValue, 0)
    }
  }

  def main(args: Array[String]): Unit = {
    require(
      args.length >= 2,
      "Uso: Main <input-csv> <output-dir> [numPartitions] [workersLabel]"
    )

    val inputPath = args(0)
    val outputPath = args(1)
    val numPartitions = if (args.length > 2) args(2).toInt else 32
    val workersLabel = if (args.length > 3) args(3) else "unknown"

    val sparkConf = new SparkConf().setAppName("Earthquake Application")
    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    val data = spark.read
      .option("header", value = true)
      .csv(inputPath)
      .rdd

    val parsed: RDD[(Int, Int)] = data.map(normalizeEvent)

    // Prende gli elementi con la stessa data e li combina in un unico valore con un accumulatore
    val cellsByDate: RDD[(Int, Array[Int])] = parsed
      .combineByKey(
        (c: Int) => mutable.HashSet(c),                                  // se incontra il primo elemento di una chiave crea l'accumulatore
        (set: mutable.HashSet[Int], c: Int) => { set += c; set },        // aggiunge un elemento a un accumulatore esistente
        (a: mutable.HashSet[Int], b: mutable.HashSet[Int]) => { a ++= b; a }, // unisce due accumulatori di partizioni diverse
        numPartitions
      )
      .filter { case (_, set) => set.size >= 2 }
      .mapValues { set =>
        val arr = set.toArray
        java.util.Arrays.sort(arr)
        arr
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    //Per ogni cella costruisce una stripe con il numero di co-occorrenza verso tutte le altre celle con cui condivide
    // almeno una data, poi somma i conteggi delle stripe della stessa cella provenienti da date diverse
    val stripes: RDD[(Int, mutable.HashMap[Int, Int])] = cellsByDate
      .flatMap { case (_, cells) =>
        val n = cells.length
        Iterator.tabulate(n - 1) { i =>
          val stripe = new mutable.HashMap[Int, Int]()
          var j = i + 1
          while (j < n) { stripe += (cells(j) -> 1); j += 1 }
          (cells(i), stripe)
        }
      }
      .reduceByKey(
        (m1, m2) => {                  // unione di due stripe con somma dei conteggi chiave per chiave
          m2.foreach { case (k, v) => m1 += (k -> (m1.getOrElse(k, 0) + v)) }
          m1
        },
        numPartitions
      )

    // Coppia con il massimo numero di co-occorrenze
    val (cellA, (cellB, maxCount)) = stripes
      .map { case (a, stripe) => (a, stripe.maxBy(_._2)) }   // massimo locale dentro ogni stripe
      .reduce { (x, y) => if (x._2._2 >= y._2._2) x else y } // massimo globale tra le stripe

    // Date della coppia vincente
    val bestDates: Array[String] = cellsByDate
      .filter { case (_, cells) =>
        java.util.Arrays.binarySearch(cells, cellA) >= 0 &&
          java.util.Arrays.binarySearch(cells, cellB) >= 0
      }
      .keys
      .collect()
      .sorted
      .map(dateIntToString)

    cellsByDate.unpersist()

    val durationSeconds = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Tempo di esecuzione: ${durationSeconds}s")
    println(s"Co-occorrenze trovate: $maxCount")

    val out = new StringBuilder
    val pairText = s"(${formatCell(cellA)}, ${formatCell(cellB)})"
    out.append(pairText + "\n")
    bestDates.foreach(d => out.append(d).append('\n'))
    println(out.toString())

    appendRunOutput(
      outputPath,
      sc.hadoopConfiguration,
      numPartitions,
      workersLabel,
      durationSeconds,
      pairText,
      maxCount,
      bestDates
    )

    spark.stop()
  }

  // Salvataggio del risultato
  def appendRunOutput(
                       outputPath: String,
                       hadoopConf: org.apache.hadoop.conf.Configuration,
                       numPartitions: Int,
                       workersLabel: String,
                       durationSeconds: Double,
                       pairText: String,
                       maxCount: Int,
                       dates: Array[String]
                     ): Unit = {

    val timeStr = f"$durationSeconds%.3f".replace('.', ',')
    val datesStr = dates.mkString(" ")
    val newEntry = s"partitions=$numPartitions | workers=$workersLabel | time=${timeStr}s | pair=$pairText | co-occurrences=$maxCount | dates=$datesStr\n"

    val path = new Path(s"$outputPath/output.txt")
    val fs = resolveFileSystem(path, hadoopConf)

    val previousContent =
      if (fs.exists(path)) {
        val in = fs.open(path)
        try scala.io.Source.fromInputStream(in, "UTF-8").mkString
        finally in.close()
      } else ""

    val fullContent = previousContent + newEntry

    val stream = fs.create(path, true) // overwrite con il contenuto completo
    try {
      stream.write(fullContent.getBytes(StandardCharsets.UTF_8))
    } finally {
      stream.close()
    }
    println(s"Risultato in: $path")
  }

  private def resolveFileSystem(path: Path, hadoopConf: org.apache.hadoop.conf.Configuration): FileSystem = {
    FileSystem.get(path.toUri, hadoopConf) match {
      case local: org.apache.hadoop.fs.LocalFileSystem => local.getRawFileSystem
      case other => other
    }
  }
}