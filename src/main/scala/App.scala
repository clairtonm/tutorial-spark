import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.functions._

import scala.Titanic
import org.apache.spark.sql.Encoders
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.types.DoubleType


/**
 * "Fast and general engine for large-scale processing"
 *
 * 1 - Develop driver program
 * 2 - Run on cluster Spark/Yarn/Mesos
 * 3 - Split the job among the nodes
 */

object App extends App {

    System.setProperty("hadoop.home.dir", "C:/hadoop/")

//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)

    /**
        1- Initialize Spark Session 
    **/
//    val spark = SparkSession.builder()
//                            .appName("Tutorial Spark")
//                            .master("local[3]")
//                            .config("spark.executor.memory", "2g")
//                            .getOrCreate()

val spark = SparkSession.builder()
  .appName("Tutorial Spark")
  .master("spark://192.168.56.1:7077")
  .getOrCreate()

    import spark.implicits._

    /**
        2 - Read the Titanic CSV
    **/
    val schema = Encoders.product[Titanic].schema
    val titanicDS = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("./data/train.csv")

    titanicDS.show()
    titanicDS.printSchema()

    /**
        3 - Perform some analysis
    **/
    titanicDS.describe().show()

    println(" - Top 5 Age and Sex count - ")
    titanicDS.groupBy("Age").count().orderBy(desc("Count")).show(5)
    titanicDS.groupBy("Sex").count().show()

    println(" - Correlation between columns - ")
    println("Corr Age/Survived: " + titanicDS.stat.corr("Age", "Survived"))
    println("Corr Pclass/Survived: " + titanicDS.stat.corr("Pclass", "Survived"))
    println("Corr Fare/Age: " + titanicDS.stat.corr("Fare", "Age"))

    /**
     * Transform string Sex feature into numeric type and calc the correlation
     */
    val stringIndexer: StringIndexer = new StringIndexer()
        .setInputCol("Sex")
        .setOutputCol("indexed_sex")

    val stringIndexerDF = stringIndexer.fit(titanicDS).transform(titanicDS)
    stringIndexerDF.show(5)
    stringIndexerDF.select("indexed_sex").printSchema()

   println("Corr Fare/Sex: " + stringIndexerDF.stat.corr("Fare", "indexed_sex"))


    println("\n - Top Age with sex and count information - ")
    titanicDS.groupBy("Sex", "Age").count().orderBy(desc("Age")).show(5)

    spark.stop()
}