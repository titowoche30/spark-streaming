package part4integrations

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import common._

object IntegratingKafka {
    val spark: SparkSession = SparkSession.builder()
      .appName("Integrating Kafka")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    def readFromKafka(): Unit = {
        val kafkaDF: DataFrame = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers","localhost:9092")
          .option("subscribe","rockthejvm")             // to more topics just add: "rockthejvm, other topic"
          .load()


        kafkaDF
          .select(col("topic"), expr("cast(value as string) as actualValue"))
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()
    }

    def writeToKafka(): Unit = {
        val carsDF = spark.readStream
          .schema(carsSchema)
          .json("src/main/resources/data/cars")

        // To be Kafka compatible, it needs to have a key and a value
        val carsKafkaDF = carsDF.selectExpr("upper(Name) as key","Name as value")

        carsKafkaDF.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers","localhost:9092")
          .option("topic","rockthejvm")
          .option("checkpointLocation","checkpoints")           // without checkpoints the writing to Kafka will fail
          .start()
          .awaitTermination()

    }

    // write the whole cars data structure to Kafka as JSON
    def writeCarsToKafka(): Unit = {
        val carsDF = spark.readStream
          .schema(carsSchema)
          .json("src/main/resources/data/cars")

        val carsJsonKafkaDF = carsDF.select(
            col("Name").as("key"),
            to_json(struct(col("Name"),col("Horsepower"),col("Origin")))
              .cast("String").as("value")
        )

        carsJsonKafkaDF.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers","localhost:9092")
          .option("topic","rockthejvm")
          .option("checkpointLocation","checkpoints")
          .start()
          .awaitTermination()
    }


    def main(args: Array[String]): Unit = {
        //readFromKafka()
        //writeToKafka()
        writeCarsToKafka()
    }
}
