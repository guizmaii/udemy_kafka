name := "scala-github-kafka-connect"

version := "0.1"

scalaVersion := "2.12.10"

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

libraryDependencies += "dev.zio"                %% "zio"                % "1.0.0-RC16"
libraryDependencies += "dev.zio"                %% "zio-kafka"          % "0.3.2"
libraryDependencies += "org.apache.kafka"       % "connect-api"         % "2.3.1"

disableScalacFlag("-Ywarn-dead-code")
disableScalacFlagInTest("-Xfatal-warnings")

def disableScalacFlag(flag: String)       = scalacOptions := scalacOptions.value.filter(_ != flag)
def disableScalacFlagInTest(flag: String) = Test / scalacOptions := scalacOptions.value.filter(_ != flag)
