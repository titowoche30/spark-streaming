package part2structuredstreaming

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import common._

object StreamingDatasets {
    val spark = SparkSession.builder()
      .appName("Streaming Datasets")
      .config("spark.master","local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    def readCars() = {
       // val carEncoder = Encoders.product[Car]
        spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()               //DF iwth single string column called "value"
          .select(from_json(col("value"),carsSchema).as("car"))        // Composite column(struct)
          .selectExpr("car.*")          //DF with multiple columns
          .as[Car]
//          .as[Car](carEncoder)                //Encoder can be passed implicitly

    }

    def showCarNames() = {
        val carsDS: Dataset[Car] = readCars()

        //transformations
        val carNamesDF: DataFrame = carsDS.select(col("Name"))

        // colecction transformations maintain type info
        val carNamesAlt: Dataset[String] = carsDS.map(_.Name)

        carNamesAlt.writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()

    }

    def showPowerfulCars() = {
        val cars = readCars()

        //transformation
        val carHPs = cars.filter(_.Horsepower.getOrElse(0L) > 140)

        carHPs.writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()

    }

    def avgHP() = {
        val cars = readCars()

        //transformation
        val carHPs = cars.select(avg(col("Horsepower")))
        carHPs.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()

    }

    def countCarsByOrigin() = {
        val cars = readCars()

        //transformation
        val carsOrigin = cars.groupBy("Origin").count()
       // val carsOrigin = cars.groupByKey(car => car.Origin).count()       maintaining dataset
        carsOrigin.writeStream
          .format("console")
          .outputMode("complete")
          .start()
          .awaitTermination()

    }

    def main(args: Array[String]): Unit = {
        //showCarNames()
        //showPowerfulCars()
//        avgHP()
        countCarsByOrigin()
    }
}
