name := "Tutorial Spark"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion
)