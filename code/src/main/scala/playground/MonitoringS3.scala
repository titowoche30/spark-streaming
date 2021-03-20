package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, input_file_name, lit, struct, to_timestamp, when,split,countDistinct}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame

object MonitoringS3 extends App{
    val spark = SparkSession.builder()
      .appName("S3 Monitor")
      .master("local[6]")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("fs.s3a.access.key", "AKIAZBU3EXWHEWLG57YY")
      .config("fs.s3a.secret.key", "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")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .getOrCreate()

    spark.conf.set("spark.sql.streaming.schemaInference", true)
    spark.sparkContext.setLogLevel("ERROR")

    def readStreamS3(): Unit = {
        val table = "LOCATION"
        //val path = s"s3a://claudemir-spark-teste-bucket-2/$table/*"
        val path = s"s3a://claudemir-spark-teste-bucket/teste-versionamento/$table/*"
        val dataFrame = spark.readStream
          .option("mergeSchema", "true")
          .option("maxFilesPerTrigger", 10)
          .parquet(path)
          .withColumn("FileName", input_file_name())

        val lineCount = dataFrame.select("FileName")
          .distinct()
          .withColumn("_tmp", split(col("FileName"), "/"))
          .select(
              col("_tmp").getItem(3).as("col1"),
              col("_tmp").getItem(4).as("col2"),
              col("_tmp").getItem(5).as("col3"),
          )

        lineCount.writeStream
          .format("console")
          .outputMode("update")
          .start()
          .awaitTermination()
    }

    readStreamS3()


}
