import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.nio.charset.StandardCharsets

object Main {
  def main(args: Array[String]): Unit = {
    //2 argomenti: path di input e output
    if (args.length < 2) {
      System.err.println("Usage: Main <input-path> <output-folder>")
      System.exit(1)
    }
    val filename = args(0)
    val outputPath = args(1)
    val numPartitions = if (args.length > 2) args(2).toInt else 32
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

    val data = spark.read
      .option("header", value = true) // Prima riga del csv non contiene dati
      .csv(filename)
      .rdd

    // 1.1. Formato del Dataset
    val cleanedData = data.map(row => {
      val lat = row.getAs[String]("latitude").toDouble
      val lon = row.getAs[String]("longitude").toDouble
      val date = row.getAs[String]("date").substring(0, 10) // Prende solo YYYY-MM-DD (finestra temporale di 1 giorno)

      // Arrotondo alla prima cifra decimale
      val latRound = BigDecimal(lat).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val lonRound = BigDecimal(lon).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble

      ((latRound, lonRound), date)
    }).distinct() // Rimuove duplicati nello stesso giorno

    // 1.2. Formato del risultato dell’analisi
    val events = cleanedData.toDF("location", "date")
      .repartition(numPartitions) // Trasformo l'RDD in un DataFrame e lo partiziona in cluster,
    //.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)// Rendo i dati persistenti per evitare ricalcoli durante il join


    // Analisi di co-occorrenza (Map-Reduce Distributed Task)
    val results = events.as("a")
      .join(events.as("b"), col("a.date") === col("b.date"))
      .filter(col("a.location") < col("b.location"))
      .groupBy(col("a.location").as("loc_a"), col("b.location").as("loc_b"))
      .agg(count("*").as("count"), collect_list("a.date").as("dates"))
      //.orderBy(desc("count"))
      //.persist() // Evita ricalcoli per .write e .first()

    val topResult = results.rdd.max()(Ordering.by(_.getAs[Long]("count"))) // Prendo risultato con massimo numero di occorrenze

    //tempo finale
    val t1 = System.nanoTime()
    val durationSeconds = (t1 - t0) / 1e9

    println(s"(${topResult.get(0)}, ${topResult.get(1)})")

    val datesList = topResult.getList[String](3).toArray.map(_.toString).sorted // Estraggo la lista delle date e le ordino in modo crescente
    datesList.foreach(println)
    //println(s"Le date della co-occorrenza più frequente sono ${datesList.length}")

    println(s"Tempo di esecuzione: $durationSeconds secondi")
    println(s"Coppia Max: (${topResult.get(0)}, ${topResult.get(1)}) con ${topResult.get(2)} occorrenze")

    // Salva nel txt
    val locA = topResult.get(0)
    val locB = topResult.get(1)
    val datesSorted = topResult.getList[String](3).toArray.map(_.toString).sorted

    val outputSnippet = s"(($locA),($locB)) | ${datesSorted.take(10).mkString(" | ")}..."
    val logEntry = s"""{"workers": "$workers", "partitions": $numPartitions, "time_seconds": $durationSeconds, "output_snippet": "$outputSnippet"}\n${"-" * 40}\n"""

    val directory = Paths.get(outputPath)
    val filePath = directory.resolve("results.txt")

    try {
      if (!Files.exists(directory)) Files.createDirectories(directory)

      Files.write(
        filePath,
        logEntry.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND
      )
      println(s"Esperimento salvato in: ${filePath.toAbsolutePath}")
    } catch {
      case e: Exception => println(s"Errore salvataggio: ${e.getMessage}")
    }

    spark.stop()
  }
}