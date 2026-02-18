# Guida

## Configurazione IntelliJ IDEA

### Scala
- Apri impostazioni di IntelliJ e seleziona Plugins
- Cerca "Scala" nel marketplace e installalo
- Riavvia l'IDE.

### Creazione progetto
- Nella barra in alto seleziona File->New->Project
- Seleziona Scala nella colonna a sinistra e poi sbt a destra.

### file sbt
- Apri il file build.sbt e incolla:

```
name := "Progetto_Terremoti"

version := "0.1"

// Ti consiglio la 2.12.18 perché è la più usata per Spark in ambito accademico
scalaVersion := "2.12.18"

// Queste righe dicono a IntelliJ di scaricare Spark per te
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.5.0",
"org.apache.spark" %% "spark-sql"  % "3.5.0"
)
```
- clicca l'esagono a sinistra (sbt)
- clicca le 2 freccie in alto a sinistra (Sync All sbt Projects)

### per cambiare versione di jdk
- Vai su File > Project Structure > Project.
- Sotto SDK, clicca sul menu a tendina e seleziona Add SDK > Download JDK.
- Scegli la versione 11 (qualsiasi vendor come Amazon Corretto o Eclipse Temurin va bene).
- Una volta scaricato, selezionalo come SDK del progetto e clicca OK.
- Riprova a lanciare il programma.

## Progetto
- inizia a scrivere in src/main/scala/main.scala
- per farlo andare in locale ho aggiunto queste righe in alto da main -> Edit configurations -> Programming arguments:
  ```
  "/Users/sara/Documents/UNIBO/Scalable/Progetto_Terremoti/Datasets/dataset-earthquakes-trimmed.csv"
  "/Users/sara/Documents/UNIBO/Scalable/Progetto_Terremoti/Risultati"
    ```
## Cluster
Apri il terminale in basso a sinistra e scrivi
```
sbt package
```
Questo crea il file JAR da caricare sul cloud (ricorda di mettere dataset completo)

## gcloud
https://console.cloud.google.com/welcome?_gl=1*q0m9ia*_up*MQ..&gclid=Cj0KCQiA4eHLBhCzARIsAJ2NZoLAp02TKiKTpx1l1meEVKtWzjqaD4q5xieRCY1fk_Ls9Bf3dqewOjUaAnKqEALw_wcB&gclsrc=aw.ds&project=earthquake-application-485610
1. Creare il Progetto su Google Cloud
- Vai sulla Google Cloud Console e accedi. 
- In alto a sinistra, clicca sul menu a tendina dei progetti e seleziona "Nuovo progetto". 
- Dai un nome al progetto (Earthquake Application) e clicca su Crea.

2. Attivare la Fatturazione (Billing)
- Nel menu a sinistra (le tre linee), vai su Fatturazione (Billing).
- Assicurati che ci sia un account di fatturazione attivo. Se non c'è, segui la procedura per riscattare il coupon universitario.

3. Abilitare le API 
- Nella barra di ricerca in alto, scrivi "Cloud Storage API", cliccaci e premi Abilita. 
- Cerca "Cloud Dataproc API" e abilita. 
- Cerca "Compute Engine API" e abilita.

4. Creare il Bucket (Storage)
- Vai su Cloud Storage > Buckets. 
- Clicca su Crea. 
- Scegli un nome unico (nome: earthquake_application). 
- Regione: Scegli regione:us-east1 (Carolina del Sud). 
- Clicca su Crea. 
- Carica i file: Carica il tuo file .jar (creato con sbt package) e il file .csv completo dei terremoti.

5. Crea i cluster con 2, 3 e 4 workers
- Usa la shell in alto a destra e incolla: 
```
gcloud services enable cloudresourcemanager.googleapis.com --project=earthquake-application-485610 
```
- Dai i permessi a dataproc worker
```
gcloud projects add-iam-policy-binding earthquake-application-485610 \
  --member="serviceAccount:1087407326383-compute@developer.gserviceaccount.com" \
  --role="roles/dataproc.worker"
```
- Dai i permessi per il bucket
```
gcloud projects add-iam-policy-binding earthquake-application-485610 \
--member="serviceAccount:1087407326383-compute@developer.gserviceaccount.com" \
--role="roles/storage.admin"
```
Crea il bucket e carica i file
Comando per creare i cluster (modifica num-workers)
```
gcloud dataproc clusters create earthquake-application \
--region=us-east1 \
--num-workers 2 \
--master-boot-disk-size 240 \
--worker-boot-disk-size 240 \
--master-machine-type=n2-standard-4 \
--worker-machine-type=n2-standard-4 \
--project=earthquake-application-485610
```
- esegui il mio codice (il path di output andrà cambiato a seconda dei workers)
```
gcloud dataproc jobs submit spark \
--cluster=earthquake-application \
--region=us-east1 \
--jar=gs://b-earthquake/progetto_terremoti_2.12-0.1.jar \
-- gs://b-earthquake/dataset-earthquakes-full.csv gs://b-earthquake/Risultati_Test_2W
```
- elimina il cluster
```
gcloud dataproc clusters delete earthquake-application --region us-east1
```







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
val logEntry = s"""{"workers": "$workers", "partitions": $parts, "time": $time, "output": "($lA,$lB)"}\n"""
val directory = Paths.get(path)
if (!Files.exists(directory)) Files.createDirectories(directory)
Files.write(directory.resolve("results.txt"), logEntry.getBytes(StandardCharsets.UTF_8),
StandardOpenOption.CREATE, StandardOpenOption.APPEND)
}
}