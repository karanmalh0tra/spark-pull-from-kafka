name := "Kafka-Spark-Integration"

version := "0.1"

resolvers += Resolver.mavenLocal

scalaVersion := "2.12.9"
val logbackVersion = "1.3.0-alpha10"
val sfl4sVersion = "2.0.0-alpha5"
val typesafeConfigVersion = "1.4.1"
val kafkaVersion = "2.8.0"
val circeVersion = "0.14.1"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "protocol-core" % "2.17.89",
  "software.amazon.awssdk" % "ses" % "2.17.89",
  "com.sun.mail" % "javax.mail" % "1.6.2",
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.slf4j" % "slf4j-api" % sfl4sVersion,
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.apache.spark" %% "spark-core" % "3.0.3",
  "org.apache.spark" %% "spark-streaming" % "3.0.3",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.3",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.mockito" % "mockito-core" % "4.0.0" % Test,
  "junit" % "junit" % "4.12",
  "com.novocode" % "junit-interface" % "0.10" % Test
)
