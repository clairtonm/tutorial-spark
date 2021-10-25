name := "Tutorial Spark"

version := "0.2"

scalaVersion := "2.12.15"

val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-avro" % sparkVersion,
    "org.apache.hudi" %% "hudi-spark3-bundle" % "0.9.0",
    "org.apache.httpcomponents" % "httpclient" % "4.5.9"


)