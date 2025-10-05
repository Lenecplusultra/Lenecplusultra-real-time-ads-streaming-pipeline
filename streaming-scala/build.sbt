ThisBuild / scalaVersion := "2.12.18"   // Spark 3.5 works best with 2.12
val sparkV = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkV,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
  "org.postgresql" % "postgresql" % "42.7.3"
)

Compile / run / fork := true

Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
)

ThisBuild / organization := "com.texyonzo"
ThisBuild / version := "0.1.0"
