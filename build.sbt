name := "reactive-streams-with-monix"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "io.monix" %% "monix-reactive" % "2.3.0",
  "io.monix" %% "monix-nio" % "0.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.2.0"
)
