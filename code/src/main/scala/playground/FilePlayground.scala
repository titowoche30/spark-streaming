package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, to_timestamp,struct, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.DataFrame
import org.apache.avro.Schema

case class Table(primaryKey: String = "ID", schema: String, name: String) {
    val topic: String = s"$schema.$name".toLowerCase
    val nameToLowerCase: String = name.toLowerCase
}

object FilePlayground extends App {
    val spark = SparkSession.builder()
      .appName("Spark Essentials Playground App")
      .master("local[6]")
      .config("spark.sql.broadcastTimeout", "36000")
      .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
      .config("fs.s3a.access.key", "ASIAWBZADJJHBCQH6SOC")
      .config("fs.s3a.secret.key", "fVGjc1cb2CiKhmy6dbgCGD69BdboYfzrv3D9pBPW")
      .config("fs.s3a.session.token", "FwoGZXIvYXdzECYaDDMw72QrWq//kqm1giLdARVdPqbTwtzNeejc9AGg3EnggDatsaA+kohOC0+Xum+dvv8BNn4ZMrbt5jD/ES+vEcqtYSTTu1Zmsldj+f00EC9H/Ta+kqYENAeDHuyaQgY/Dn30dqq5aEMwYljxcPpEy0FSwgYkvyiOXRm6mkUmPYVaJhFTk7Kv4Vi1wE/LUeY1ndn+C0Za78YHQz4kvCP/10rdvpWl5jQ5alrZcOmoznXeHhRwHDtNwEn+9tYmIqhQQCrFtqymW/598Vn9ez45kqH6hvpyKZ6OYsrJcUiGT19FRL++mosYwIcN1tT4KNv7jP8FMjKxN1hh7dTwEn+6oiArsD3teXduQ0lZN94kUAltIk4CuKe3q7OwVkd0LvpOgloZET8Ftw==")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
      .getOrCreate()

    spark.conf.set("spark.sql.streaming.schemaInference", true)
    spark.sparkContext.setLogLevel("ERROR")

    def readStreamS3(): Unit = {
        //Delete e Archive
        //  Funfou: users,userdriver, driver, stopsignature
        // N-funfou: na stoptype que só tem 1 parquet, não foi


        val table = "STOPTYPE"
        val path = s"s3a://gm-analytics/gm-analytics-etl/test_jar/claudemir/$table/*"
        val path2 = "s3a://gm-analytics/gm-analytics-etl/test_jar/outro_claudemir/"
        val dataFrame = spark.readStream
          .option("mergeSchema", "true")
          .option("maxFilesPerTrigger", 1)
          .option("cleanSource", "archive")
          .option("sourceArchiveDir", path2) // path2/path
          .parquet(path)

        val lineCount = dataFrame.selectExpr("count(*) as lineCount")
        lineCount.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()
    }

    readStreamS3()
}

//    def avroWriter(table: Table, dataFrame: DataFrame): Unit = {
//        val overrideTypes = List(OverrideAvro("timestamp-micros", "timestamp-millis", Map("connect.name" -> "org.apache.kafka.connect.data.Timestamp")))
//        val kafkaMessageDF = prepareKafkaMessageFormat(table, dataFrame)
//        val valueJsonAvroSchema = overrideAvroSchemaValue(table, dataFrame, overrideTypes)
//        val avroDF = transformKafkaMessageToAvro(table, valueJsonAvroSchema, kafkaMessageDF)
//
//        avroDF.writeStream
//          .format("kafka")
//          .queryName(s"${table.schema}/${table.name}")
//          .option("kafka.bootstrap.servers", analyticsEtlConfig.brokersUrl)
//          .option("topic", table.topic)
//          //  .option("checkpointLocation", s"/${analyticsEtlConfig.checkpointDir}/${table.schema}/${table.name}")
//          .option("checkpointLocation", s"${analyticsEtlConfig.fsPrefix}/checkpoints/${table.schema}/${table.name}")
//          // .option("checkpointLocation", s"checkpoints/${table.schema}/${table.name}")
//          .start()
//          .awaitTermination()
//    }

//}
