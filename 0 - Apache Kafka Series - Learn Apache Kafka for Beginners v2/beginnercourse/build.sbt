name := "beginnercourse"

version := "0.1"

scalaVersion := "2.13.0"

ThisBuild / turbo := true
ThisBuild / scalafmtOnCompile := true
ThisBuild / scalafmtCheck := true
ThisBuild / scalafmtSbtCheck := true

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.28"