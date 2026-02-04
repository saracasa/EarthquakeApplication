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


