name := "Progetto_Terremoti"

version := "0.1"

scalaVersion := "2.12.18"

// Queste righe dicono a IntelliJ di scaricare Spark per te
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0"
)