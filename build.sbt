name := "SparkKmeans"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0" exclude("org.scala-lang", "scala-compiler") exclude("org.scala-lang", "scala-reflect") exclude("org.scala-lang.modules", "scala-parser-combinators_2.11")