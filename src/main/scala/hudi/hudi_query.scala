package hudi

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig._

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object HudiQuery extends App {
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val tableName = "hudi_trips_cow"
    val basePath = "file:///home/clairtonm/Projects/tutorial-spark/data/hudi_trips_cow"
    val dataGen = new DataGenerator

    val spark = SparkSession.builder()
                .master("spark://localhost:7077")
                .appName("Hudi Test")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()

    val tripsSnapshotDF = spark.
        read.
        format("hudi").
        load(basePath)
        
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

    tripsSnapshotDF.show()

    print(tripsSnapshotDF.count())
}