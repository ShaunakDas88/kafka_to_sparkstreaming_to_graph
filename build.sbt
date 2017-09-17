name := "kafka_to_spark_streaming_to_graph"

version := "0.1"

scalaVersion := "2.11.8"

val dseVersion = "5.1.1"

resolvers += "DataStax Repo" at "https://datastax.artifactoryonline.com/datastax/dse/"

mainClass in (Compile, packageBin) := Some("com.datastax.kafka_to_sparkstreaming_to_graph.AmazonStreamedFromKafka")

// Warning Sbt 0.13.13 or greater is required due to a bug with dependency resolution
libraryDependencies += "com.datastax.dse" % "dse-spark-dependencies" % dseVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.1.1"
