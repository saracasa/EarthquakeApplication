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

    // Inizializzazione Sessione Spark
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

    // 1.2. Analisi delle co-occorrenze
    val events = cleanedData.toDF("location", "date")
      .repartition(numPartitions) // Trasformo l'RDD in un DataFrame e lo partiziona in cluster,

    // Analisi di co-occorrenza (Map-Reduce Distributed Task)
    val results = events.as("a")
      .join(events.as("b"), col("a.date") === col("b.date"))
      .filter(col("a.location") < col("b.location"))
      .groupBy(col("a.location").as("loc_a"), col("b.location").as("loc_b"))
      .agg(count("*").as("count"), collect_list("a.date").as("dates"))

    val topResult = results.rdd.max()(Ordering.by(_.getAs[Long]("count"))) // Prendo risultato con massimo numero di occorrenze

    //tempo finale
    val t1 = System.nanoTime()
    val durationSeconds = (t1 - t0) / 1e9

    println(s"(${topResult.get(0)}, ${topResult.get(1)})")

    val datesList = topResult.getList[String](3).toArray.map(_.toString).sorted // Estraggo la lista delle date e le ordino in modo crescente
    datesList.foreach(println)

    //Output
    val locA = topResult.get(0)
    val locB = topResult.get(1)
    val datesSorted = topResult.getList[String](3).toArray.map(_.toString).sorted

    println(s"(($locA), ($locB))")
    datesSorted.foreach(println)
    println(s"Tempo di esecuzione: $durationSeconds secondi")

    // Salva nel txt
    saveResults(outputPath, locA, locB, datesSorted, durationSeconds, numPartitions, workers)

    spark.stop()
  }

  def saveResults(path: String, lA: Any, lB: Any, dates: Array[String], time: Double, parts: Int, workers: String): Unit = {
    val logEntry = s"""{"workers": "$workers", "partitions": $parts, "time_seconds": $time, "pair": "($lA,$lB)"}\n"""
    val directory = Paths.get(path)
    if (!Files.exists(directory)) Files.createDirectories(directory)
    Files.write(directory.resolve("results.txt"), logEntry.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  }
}