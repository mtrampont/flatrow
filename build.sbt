import sbt._


val sparkVersion = "2.1.2"

val commonSettings = Seq(
  version := "0.0",
  scalaVersion := "2.11.12",
  scalacOptions += "-Ypartial-unification", // useful when using typeclasses
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

    "org.scalatest" %% "scalatest" % "3.0.4" % "test"
  )
)

lazy val macros =
  (project in file("macros"))
    .dependsOn(row % "compile->compile;test->test")
    .settings(commonSettings : _*)
    .settings(
      name := "flatrow-macros",
      libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
    )


lazy val row =
  (project in file("row"))
    .settings(commonSettings: _*)
    .settings(
      name := "flatrow-core",
      libraryDependencies += "org.typelevel" %% "cats-macros" % "1.0.1"
    )

lazy val root =
  (project in file("."))
    .dependsOn(macros, row)
    .aggregate(macros, row)
    .settings(commonSettings: _*)
    .settings(
      name := "flatrow"
    )



