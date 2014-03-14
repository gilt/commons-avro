gilt.GiltProject.jarSettings

name := "commons-avro"

scalaVersion := "2.10.3"

// annoying issue with pickling in sonatype's repo
checksums in update := Nil

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.7.6",
  "org.scala-lang" %% "scala-pickling" % "0.8.0-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "2.1.0" % "test",
  "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
)
