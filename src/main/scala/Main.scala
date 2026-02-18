import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets

object Main {
  def main(args: Array[String]): Unit = {
    //2 argomenti: path di input e output
    if (args.length < 2) {
      System.err.println("Usage: Main <input-path> <output-folder> [partitions] [workers]")
      System.exit(1)
    }
    val filename = args(0)
    val outputPath = args(1)
    val numPartitions = if (args.length > 2) args(2).toInt else 8
    val workers = if (args.length > 3) args(3) else "unknown"

    // Inizializzazione Spark
    val spark = SparkSession.builder
      .appName("Earthquake Application")
      .master("local[*]") // locale
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR") // Rimuove log (escluso errori)

    // tempo iniziale
    val t0 = System.nanoTime()

    //Caricamento RDD
    val data = spark.read
      .option("header", value = true) // Prima riga del csv non contiene dati
      .csv(filename)
      .rdd

    // 1.1. Formato del Dataset
    val cleanedData = data.map(row => {
      val lat = row.getAs[String]("latitude").toDouble
      val lon = row.getAs[String]("longitude").toDouble
      val date = row.getAs[String]("date").substring(0, 10) // YYYY-MM-DD

      // Arrotondo prima cifra decimale
      val latRound = BigDecimal(lat).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val lonRound = BigDecimal(lon).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble

      ((latRound, lonRound), date)
    }).distinct() // Rimuove duplicati nello stesso giorno

    // 1.2. Analisi delle co-occorrenze (Map-Reduce su RDD)
    // MAP: invertire chiave per raggruppare per data
    val byDate = cleanedData
      .map { case (loc, date) => (date, loc) }
      .repartition(numPartitions)

    // REDUCE: per ogni data genera tutte le coppie, poi aggrega per coppia
    val pairs = byDate
      .groupByKey()
      .flatMap { case (date, locs) =>
        val locList = locs.toList.distinct
        for {
          i <- locList.indices
          j <- (i+1) until locList.size
          a = locList(i)
          b = locList(j)
          // Ordinamento lessicografico esplicito
          (locA, locB) = if (a._1 < b._1 || (a._1 == b._1 && a._2 < b._2)) (a, b) else (b, a)
        } yield ((locA, locB), date)
      }

    // reduceByKey aggrega localmente prima di shufflare (più efficiente)
    val cooccurrences = pairs
      .map { case (pair, date) => (pair, Set(date)) }
      .reduceByKey(_ ++ _)
      .map { case (pair, dates) => (pair, dates.toList.sorted) }

    val topResult = cooccurrences
      .map { case (pair, dates) => (pair, dates, dates.size) }
      .reduce { (a, b) => if (a._3 >= b._3) a else b }

    //tempo finale
    val t1 = System.nanoTime()
    val durationSeconds = (t1 - t0) / 1e9

    //Output
    val ((locA, locB), datesSorted, _) = topResult

    println(s"(($locA), ($locB))")
    datesSorted.foreach(println)
    println(s"Tempo di esecuzione: $durationSeconds secondi")

    // Salva nel txt
    saveResults(outputPath, locA, locB, datesSorted.toArray, durationSeconds, numPartitions, workers)

    spark.stop()
  }

  def saveResults(path: String, lA: Any, lB: Any, dates: Array[String], time: Double, parts: Int, workers: String): Unit = {
    val logEntry = s"""{"workers": "$workers", "partitions": $parts, "time": $time, "output": "($lA,$lB)"}\n"""
    val directory = Paths.get(path)
    if (!Files.exists(directory)) Files.createDirectories(directory)
    Files.write(directory.resolve("results.txt"), logEntry.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}