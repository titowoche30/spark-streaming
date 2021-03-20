package part2structuredstreaming

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {
    val appName = "Streaming Aggregations"
    val spark = SparkSession.builder()
      .appName(appName)
  //    .config("spark.master","local[2]")
      .config("spark.ui.prometheus.enabled","true")
      .config("spark.executor.processTreeMetrics.enabled","true")
      .config("spark.metrics.appStatusSource.enabled","true")
      .config("spark.sql.streaming.metricsEnabled","true")
      .config("spark.metrics.namespace",appName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    def streamingCount() = {
        val lines =  spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()

        val lineCount = lines.selectExpr("count(*) as lineCount")

        // aggregations with distinct,sorting and others are not supported
        // otherwise Spark will need to keep track of EVERYTHING

        lineCount.writeStream
          .format("console")
          .outputMode("complete")      // append and update not suportted on aggregations without watermark
          .start()
          .awaitTermination()
    }

    def numericalAggregations(aggFunction: Column => Column) = {
        val lines =  spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()

        //aggregate
        val numbers = lines.select(col("value").cast("integer").as("number"))
        val aggDF = numbers.select(aggFunction(col("number")).as("agg_so_far"))

        aggDF.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()

    }

    def groupNames() = {
        val lines =  spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()

        // counting each occurence of the "name" value
        val names = lines
          .select(col("value").as("name"))
          .groupBy(col("name"))
          .count()

        names.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()

    }



    def main(args: Array[String]): Unit = {
        //streamingCount()
//        numericalAggregations(mean)
        groupNames()


    }

}
