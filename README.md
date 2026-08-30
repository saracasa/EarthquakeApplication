# Analisi di co-occorrenza di eventi sismici

Sara Casadio (matricola 0001186923)

Progetto per il corso di **Scalable and Cloud Programming** — Alma Mater Studiorum, Università di Bologna, A.A. 2025–2026.

## Obiettivo

Data una raccolta di eventi sismici, l'applicazione individua la **coppia di località in cui i terremoti si verificano più spesso nello stesso giorno** ed elenca in ordine crescente le date in cui tale co-occorrenza si è verificata.

L'implementazione è in Scala con Apache Spark, segue il paradigma map-reduce e usa le RDD per controllare esplicitamente le fasi di trasformazione e aggregazione.

## Struttura del repository

```
.
├── project
│   └── build.properties
├── Report
│   ├── Report.tex
│   └── Report.pdf
├── src/main/scala
│   └── Main.scala
├── build.sbt
└── README.md
```

## Requisiti

- JDK 11
- Scala 2.12.18
- sbt 1.12.0
- Apache Spark 3.5.0 (per l'esecuzione locale)
- Google Cloud SDK (per l'esecuzione su Dataproc)

## Dataset

Il dataset è un file CSV di eventi sismici contenente, per ciascun evento, latitudine, longitudine e istante temporale. Non è incluso nel repository per motivi di dimensione: va caricato su un bucket di Google Cloud Storage prima dell'esecuzione sul cluster.

## Compilazione

```bash
sbt package
```

Il jar viene prodotto in `target/scala-2.12/<nome-jar>.jar`.

## Esecuzione

Nei comandi che seguono vanno sostituiti `<bucket>` con il nome del proprio bucket di Google Cloud Storage, `<nome-jar>` con il nome del jar prodotto dalla compilazione, `<dataset>` con il nome del file CSV e `<cluster>` con il nome scelto per il cluster.

### Su Google Cloud Dataproc

Dopo aver creato un bucket:

1. Caricare dataset e jar sul bucket:

```bash
gsutil cp <dataset>.csv gs://<bucket>/
gsutil cp target/scala-2.12/<nome-jar>.jar gs://<bucket>/
```

2. Creare il cluster (l'esempio è la configurazione a 2 worker):

```bash
gcloud dataproc clusters create <cluster> \
  --region=us-east1 \
  --image-version=2.2-debian12 \
  --num-workers 2 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4 \
  --master-boot-disk-size 240 \
  --worker-boot-disk-size 240 \
  --max-idle=30m
```

3. Lanciare il job:

```bash
gcloud dataproc jobs submit spark \
  --cluster=<cluster> \
  --region=us-east1 \
  --class=Main \
  --jars=gs://<bucket>/<nome-jar>.jar \
  -- gs://<bucket>/<dataset>.csv \
     gs://<bucket>/output \
     32 2w
```

4. Eliminare il cluster al termine dei test:

```bash
gcloud dataproc clusters delete <cluster> --region=us-east1
```

L'opzione `--max-idle=30m` fa sì che il cluster si elimini da solo dopo mezz'ora di inattività, come protezione contro l'esaurimento accidentale dei crediti.

### In locale

Se non è impostato `spark.master`, l'applicazione ricade automaticamente su `local[*]`, quindi il jar è eseguibile anche fuori dal cluster:

```bash
spark-submit \
  --class Main \
  --master local[*] \
  target/scala-2.12/<nome-jar>.jar \
  <dataset>.csv output 8 locale
```

### Parametri

| Parametro | Descrizione |
|---|---|
| `input` | percorso del CSV di input (locale o `gs://`) |
| `output` | cartella in cui viene scritto `output.txt` |
| `numPartitions` | numero di partizioni usato dalle trasformazioni wide (opzionale, default 32) |
| `workersLabel` | etichetta della configurazione, riportata nel log dei risultati (opzionale, default `unknown`) |

Gli argomenti vanno passati in quest'ordine. `workersLabel` non ha effetto sul calcolo: serve solo a distinguere le righe di `output.txt` quando si eseguono più run con configurazioni diverse.

## Output

A video il programma stampa il tempo di esecuzione, il numero di co-occorrenze, la coppia vincente e l'elenco crescente delle date.

In `<output>/output.txt` viene inoltre aggiunta una riga per ogni esecuzione, nel formato:

```
partitions=16 | workers=2w | time=110,270s | pair=((38.8, -122.8), (38.8, -122.7)) | co-occurrences=10014 | dates=1990-01-05 1990-01-06 ...
```

Il file viene letto e riscritto per intero a ogni run, così i risultati delle diverse configurazioni si accumulano in un unico log confrontabile.

Sul dataset completo la coppia vincente è `((38.8, -122.8), (38.8, -122.7))` con **10 014 co-occorrenze**, distribuite su date comprese fra il 1990-01-05 e il 2023-07-29. Il risultato è identico in tutte le configurazioni di cluster e di partizionamento testate.

## Relazione

L'analisi delle prestazioni, con i tempi misurati al variare del numero di worker e di partizioni, lo studio di strong scaling e la stima della componente non parallelizzabile, è riportata nella relazione allegata.