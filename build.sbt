gilt.GiltProject.jarSettings

name := "commons-avro"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.7.6",
  // Remove when scala-pickling fix issue https://github.com/scala/pickling/issues/119 and https://github.com/scala/pickling/issues/120
  "org.scalamacros" %% "quasiquotes" % "2.0.0-M8",
  "org.scala-lang" %% "scala-pickling" % "0.8.0",
  "org.scalatest" %% "scalatest" % "2.1.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
)
