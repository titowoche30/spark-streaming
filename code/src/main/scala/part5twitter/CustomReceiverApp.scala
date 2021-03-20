package part5twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


// Creates a low level spark DStream which will read data com a custom source
object CustomReceiverApp {
    val spark: SparkSession = SparkSession.builder()
      .appName("Custom receiver app")
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

    def main(args: Array[String]): Unit = {

    }
}
