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

object Hudi extends App {

   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("akka").setLevel(Level.OFF)

    val tableName = "hudi_trips_cow"
    val basePath = "file:///home/clairtonm/Projects/tutorial-spark/data/hudi_trips_cow"
    val dataGen = new DataGenerator

    val inserts = convertToStringList(dataGen.generateInserts(100000))

    val spark = SparkSession.builder()
                .master("local")
                .appName("Hudi Test")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate()

    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
        options(getQuickstartWriteConfigs).
        option(PRECOMBINE_FIELD_OPT_KEY, "ts").
        option(RECORDKEY_FIELD_OPT_KEY, "uuid").
        option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
        option(HoodieWriteConfig.TABLE_NAME, tableName).
        mode(Overwrite).
        save(basePath)    

}

