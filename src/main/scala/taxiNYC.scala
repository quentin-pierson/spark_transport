import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


object taxiNYC extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val url_schema = "src/data/streaming_test/yellow_tripdata_2015-01_0_row_10000_ef1a0c5a-f5d8-4e04-9e46-485eec36d00c.csv"
  val url : String = "src/data/streaming_test/*.csv"

  val staticDataFrame = sparkSession.read.format("csv")
    .option("header", true)
    .option("inferSchema","true")
    .load(url_schema)

  staticDataFrame.createOrReplaceTempView("taxi_nyc")
  val staticSchema = staticDataFrame.schema

  val df = sparkSession.readStream
    .schema(staticSchema)
    .format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .option("delimiter",",")
    .load(url)

  df.printSchema()

  println("wait for data ")
  println("Streaming DataFrame : " + df.isStreaming)

  val purchaseByCustomerPerDay = df
    .selectExpr(
      "VendorID",
      "passenger_count",
    )
    .groupBy(
      col("VendorID")
    )
    .sum("passenger_count")

  purchaseByCustomerPerDay.writeStream
    .format("memory") // memory = sotre in-memory table
    .queryName("test") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()

//  select(purchase_stream)

  sparkSession.sql(
    """
      |select * from test
      |""".stripMargin)
    .show(false)

  sparkSession.stop()

//  for (i <- 1 to 10 ) {
//   sparkSession.sql(
//     """
//       |select * from purchase_stream
//       |order by `sum(passenger_count)` DESC
//       |""".stripMargin)
//     .show(false)
//   Thread.sleep(1000)
//  }
}
