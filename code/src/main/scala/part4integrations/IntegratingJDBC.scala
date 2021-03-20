package part4integrations

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import common._

object IntegratingJDBC {
    // You can't read streams from JDBC
    // You can't write to JDBC in a streaming fashion
    // but you can write batches
    // new technique: foreachBatch

    val spark: SparkSession = SparkSession.builder()
      .appName("Integrating JDBC")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5432/rtjvm"
    val user = "docker"
    val password = "docker"

    // Write stream of data in the form of a structured stream to postgres
    def writeStreamToPostgres() = {
        val carsDF = spark.readStream
          .schema(carsSchema)
          .json("src/main/resources/data/cars")

        val carsDS:Dataset[Car] = carsDF.as[Car]

        carsDS.writeStream
          .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
              // each executor can control the batch
              // bach is STATIC dataset/dataframe

              batch.write
                .format("jdbc")
                .option("driver",driver)
                .option("url",url)
                .option("user",user)
                .option("password",password)
                .option("dbtable","public.cars")
            //    .mode(SaveMode.Append)                // Se rodar de novo sem essa linha aqui, vai da erro dizendo que já existe
                .save()
          }
          .start()
          .awaitTermination()



    }


    def main(args: Array[String]): Unit = {
        writeStreamToPostgres()
    }

}
