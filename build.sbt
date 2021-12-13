//import com.github.retronym.SbtOneJar._

//oneJarSettings

name := "Scala Ecommerce Generator"

version := "1.0"

scalaVersion := "2.11.7"

val akkaVersion = "2.4.2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"              % akkaVersion,
  "com.typesafe.akka" %% "akka-stream"             % akkaVersion,

  "org.scalanlp" %% "breeze"         % "0.10",
  "org.scalanlp" %% "breeze-natives" % "0.10",

  "com.typesafe.play" %% "play-json" % "2.4.3"
)

