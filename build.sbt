ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
   .settings(
       name := "ZomatoAnalysis"
   )
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2" // % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" // % "provided"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.1.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.29"
