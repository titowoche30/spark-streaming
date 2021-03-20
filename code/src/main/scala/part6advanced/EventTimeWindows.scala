package part6advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object EventTimeWindows {
    val spark: SparkSession = SparkSession.builder()
      .appName("Custom receiver app")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val onlinePurchaseSchema = StructType(Array(
        StructField("id",StringType),
        StructField("time",TimestampType),
        StructField("item",StringType),
        StructField("quantity",IntegerType)
    ))

    def readPurchasesFromSocket() = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()
      .select(from_json(col("value"),onlinePurchaseSchema).as("purchase"))
      .selectExpr("purchase.*")

    def aggregatePurchaseBySlidingWindow() = {
        val purchasesDF = readPurchasesFromSocket()

        // window(column,window duration, slide duration)  == sliding window
        val windowByDay = purchasesDF
          .groupBy(window(col("time"),"1 day","1 hour").as("time"))     // struct column: has fields {start,end}
          .agg(sum("quantity").as("totalQuantity"))
          .select(
              col("time").getField("start").as("start"),
              col("time").getField("end").as("end"),
              col("totalQuantity")
          )


        // Quantidade dum dia x na hora y até dia x + 1 na hora y
        // Quantidade em 24 hrs
        windowByDay.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()
    }


    def aggregatePurchaseByTumblingWindow() = {
        val purchasesDF = readPurchasesFromSocket()

        // window(column,window duration) == tumbling window, slide = duration, ou seja, não tem overlap de windows
        val windowByDay = purchasesDF
          .groupBy(window(col("time"),"1 day").as("time"))     // struct column: has fields {start,end}
          .agg(sum("quantity").as("totalQuantity"))
          .select(
              col("time").getField("start").as("start"),
              col("time").getField("end").as("end"),
              col("totalQuantity")
          )


        // Quantidade dum dia x na hora y até dia x + 1 na hora y
        // Quantidade em 24 hrs
        windowByDay.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()
    }


    // Exercicios
    // 1 - Best selling product every day + quantity sold
    // 2 - Best selling product of every 24 hours + quantity sold, update every hour


    def readPurchasesFromFile() = spark.readStream
      .schema(onlinePurchaseSchema)
      .json("src/main/resources/data/purchases")


    def bestSellingEveryday() = {
        val purchasesDF = readPurchasesFromFile()

        // window(column,window duration)  == tumbling window
        val bestSelling = purchasesDF
          .groupBy(window(col("time"),"1 day").as("day"),col("item"))     // struct column: has fields {start,end}
          .agg(sum("quantity").as("totalQuantity"))
          .select(
              col("day").getField("start").as("start"),
              col("day").getField("end").as("end"),
              col("item"),
              col("totalQuantity")
          )
          .orderBy(col("day"),col("totalQuantity").desc)

        bestSelling.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()
    }


    def bestSelling24hr() = {
        val purchasesDF = readPurchasesFromFile()

        // window(column,window duration)  == tumbling window
        val bestSelling = purchasesDF
          .groupBy(window(col("time"),"1 day","1 hour").as("time"),col("item"))     // struct column: has fields {start,end}
          .agg(sum("quantity").as("totalQuantity"))
          .select(
              col("time").getField("start").as("start"),
              col("time").getField("end").as("end"),
              col("item"),
              col("totalQuantity")
          )
          .orderBy(col("start"),col("totalQuantity").desc)

        bestSelling.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()
    }


    def main(args: Array[String]): Unit = {
        // aggregatePurchaseBySlidingWindow()
//        bestSellingEveryday()
        bestSelling24hr()
    }
}
