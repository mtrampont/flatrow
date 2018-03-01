import sbt._


val sparkVersion = "2.1.2"

val commonSettings = Seq(
  name := "Row",
  version := "0.0",
  scalaVersion := "2.11.12",
  scalacOptions += "-Ypartial-unification",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
    "org.typelevel" %% "cats-macros" % "1.0.1",

    "org.scalatest" %% "scalatest" % "3.0.4" % "test"
  )
)

lazy val macros =
  (project in file("macros"))
    .settings(commonSettings : _*)


lazy val row =
  (project in file("row"))
    .settings(commonSettings : _*)


