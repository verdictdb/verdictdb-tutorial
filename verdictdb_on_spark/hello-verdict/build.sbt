import Dependencies._

scalaVersion := "2.11.1"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.5",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Hello",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "org.verdictdb" % "verdictdb-core" % "0.5.4",
      "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
    )
  )
