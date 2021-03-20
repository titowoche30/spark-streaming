package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

// Event time = when the event was produced
// Processing time = when the event arrives at the spark engine

// APIs do Spark Streaming
// Low-level (DStreams)
// High-level API (Structured Streaming)

// Spark Streaming operates on micro-batches, continuous it's currently on experimental and not recomended for production


object StreamingDataFrames{
    val spark = SparkSession.builder()
      .appName("Our first streams")
      .config("spark.master","local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    def readFromSocket() = {

        // Reading a DF
        val lines: DataFrame = spark.readStream         // Streaming DataFrame
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()

        // transformation
        val shortLines:DataFrame = lines.filter(length(col("value")) <= 5)

        // tell between a static vs a streaming DF
        println(shortLines.isStreaming)
        // Consuming a DF
        val query = shortLines.writeStream                   // Calling an action on the DF
          .format("console")
          .outputMode("append")
          .start()

        query.awaitTermination()                        // Wait for the stream to finish because the start() is assyncronous
    }

    def readFromFiles() = {
        val stocksDF: DataFrame = spark.readStream
          .format("csv")
          .option("header","false")
          .option("dateFormat","MMM d yyyy")
          .schema(stocksSchema)
          .load("src/main/resources/data/stocks")

        stocksDF.writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()
    }

    def demoTriggers() = {
        val lines: DataFrame = spark.readStream         // Streaming DataFrame
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()

        val upperLines = lines.select(upper(col("value")))

        // write the lines DF at a certain trigger

        // ProcessingTime:
        // Every 2 second run the query,
        // e.g, every 2 seconds the Dataframe is checked for
        // new data and if it has any data the transformations will be executed in a new batch
        // if it doesn't have, then will wait for the next trigger
        // In a flow longer than 2 seconds, spark will separate the data and different batches

        // Once:
        // Single batch, then terminate the process

        //Continuous (experimental):
        // Every 2 seconds create a batch with whatever you have
        upperLines.writeStream
          .format("console")
          .outputMode("append")
          .trigger(
              Trigger.ProcessingTime(2.seconds)
              //Trigger.Once()
              //Trigger.Continuous(2.seconds)
                 )
          .start()
          .awaitTermination()


    }


    def main(args: Array[String]): Unit = {
        //readFromSocket()
        //readFromFiles()
        demoTriggers()
    }


}
