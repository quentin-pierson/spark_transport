import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object test extends App{

  val sparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("streaming")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5")

  val retaildf = sparkSession
    .read
    .format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .option("delimiter",";")
    .load("src/data/test.csv")

    retaildf.printSchema()
    retaildf.show(false)

}

