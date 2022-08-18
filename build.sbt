name := "Integrate-flink-streaming-with-kafka-and-cassandra-using-scala"

version := "0.1"

scalaVersion := "2.11.2"

mainClass in compile := Some("com.knoldus.FlinkKafkaToCassandra")

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-connector-kafka" % "1.9.0",
  "org.apache.flink" %% "flink-scala" % "1.9.0",

  "org.apache.flink" %% "flink-streaming-scala" % "1.9.0" ,

  "org.apache.flink" %% "flink-clients" % "1.9.0",
  "org.apache.flink" % "flink-core" % "1.9.0",

  // cassandra
  "org.apache.flink" %% "flink-connector-cassandra" % "1.9.0",
  "de.ruedigermoeller" % "fst" % "2.50",
  "org.projectlombok" % "lombok" % "1.18.12" % "provided",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.6.4"
)

