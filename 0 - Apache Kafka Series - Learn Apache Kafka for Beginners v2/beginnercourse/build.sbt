name := "beginnercourse"

version := "0.1"

scalaVersion := "2.13.1"

ThisBuild / turbo := true
ThisBuild / scalafmtOnCompile := true
ThisBuild / scalafmtCheck := true
ThisBuild / scalafmtSbtCheck := true

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.28"
libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"
