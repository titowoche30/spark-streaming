package part6advanced

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object StatefulComputations {
    val spark: SparkSession = SparkSession.builder()
      .appName("Stateful Computation")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // records
    case class SocialPostRecord(postType: String, count:Int, storageUsed: Int)
    // grouped records
    case class SocialPostBulk(postType: String, count: Int, totalStorageUsed: Int)
    // mean of grouped records
    case class AveragePostStorage(postType: String, averageStorage: Double)

    // postType, count, storageUsed
    def readSocialUpdates(): Dataset[SocialPostRecord] = {
        spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()
          .as[String]
          .map { line =>
              val tokens = line.split(",")
              SocialPostRecord(tokens(0),tokens(1).toInt,tokens(2).toInt)
          }
    }

    def getAveragePostStorage() = {
        val socialStream = readSocialUpdates()

        socialStream
          .groupByKey(_.postType)
          .agg(sum(col("count")).as("totalCount").as[Int], sum(col("storageUsed")).as("totalStorage").as[Int])
          .selectExpr("key as postType", "totalStorage/totalCount as avgStorage")
          .writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()
    }



    def main(args: Array[String]): Unit = {
        getAveragePostStorage()
    }
}
