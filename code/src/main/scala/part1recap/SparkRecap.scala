package part1recap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkRecap extends App{
    val spark = SparkSession.builder()
      .appName("Spark Recap")
      .master("local[2]")           // Aloca 2 threads na máquina
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // The DataFrame API + Spark SQL composes the Spark Structured API

    val cars = spark.read
      .option("inferSchema","true")
      .json("src/main/resources/data/cars")

    cars.printSchema()
    cars.show()

    import spark.implicits._

    cars.select(
        col("Name"),
        $"Year",    // col("Year)
        (col("Weight_in_lbs") / 2.2).as("Weight_in_kg"),
        expr("Weight_in_lbs / 2.2").as("Weight_in_kgs_2"),
    )

    cars.selectExpr("Weight_in_lbs / 2.2")

    cars.filter(col("Origin") =!= "USA")            // ou .where

    cars.select(avg(col("Horsepower")).as("average_hp"))

    cars.groupBy("Origin").count().show()

    val guitarPlayers = spark.read.option("inferSchema","true").json("src/main/resources/data/guitarPlayers")
    val bands = spark.read.option("inferSchema","true").json("src/main/resources/data/bands")

    val guitarristsBands = guitarPlayers.join(bands,guitarPlayers.col("band") === bands.col("id"))

    // datasets = typed distributed collection of objects
    case class GuitarPlayer(id: Long, name: String, guitars:Seq[Long], band:Long)

    val guitarPlayerDS = guitarPlayers.as[GuitarPlayer]     // needs spark.implicits
    guitarPlayerDS.map(_.name).show()

    cars.createOrReplaceTempView("cars")
    spark.sql(
        """
          |select Name from cars
          |where Origin = 'USA'
          |""".stripMargin
    ).show()

    // Low-level API composed of the fundamental Spark Data Structure RDD
    // RDD = Type distributed collection of objects like DataSets,
    // except they dont have the SQL API, they only have  the functional API
    // under the hood, all the spark jobs boiled down to RDDs

    val sc = spark.sparkContext
    val numbersRDD = sc.parallelize(1 to 1000000)       // RDD[Int]

    numbersRDD.map(_ * 2)
    val numbersDF = numbersRDD.toDF("number")           //Lose the type information because DF are not typed, but gain the SQL capabilities
    val numbersDs = spark.createDataset(numbersRDD)

    val guitarPlayersRDD = guitarPlayerDS.rdd
    val carsRDD = cars.rdd              //RDD[Row]



}
