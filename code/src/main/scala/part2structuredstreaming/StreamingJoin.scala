package part2structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object StreamingJoin {
    val spark = SparkSession.builder()
      .appName("Streaming Joins")
      .config("spark.master","local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val guitarPlayers = spark.read.option("inferSchema","true").json("src/main/resources/data/guitarPlayers")
    val guitars = spark.read.option("inferSchema","true").json("src/main/resources/data/guitars")
    val bands = spark.read.option("inferSchema","true").json("src/main/resources/data/bands")

    val joinCondition = guitarPlayers.col("band") === bands.col("id")
    val guitarristsBands = guitarPlayers.join(bands,joinCondition)
    val bandsSchema = bands.schema

    def joinStreamWithStatic() = {
        // Joinar um stream DF com um static DF
        val streamedBandsDF = spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()       // a DF with a single column "value" of type string
          .select(from_json(col("value"),bandsSchema).as("band"))
          .selectExpr("band.id as id","band.name as name","band.hometown as hometown","band.year as year")

        // joins happen PER BATCH
        val streamedBandsGuitaristsDF = streamedBandsDF.join(guitarPlayers,guitarPlayers.col("band") === streamedBandsDF.col("id"))

        /*
            restricted joins:
            -streaming DF joining with static DF: the RIGHT outer join, the FULL outer join and the right/semi antijoin ARE NOT PERMITED
            - static DF with streaming DF: left outer join, full outer join, left_semi e left antijoin not permited
            Because Spark does not allow unbounded accumulation of data
         */

        streamedBandsGuitaristsDF.writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()
    }

    def joinStreamWithStream() = {
        val streamedBandsDF = spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12345)
          .load()       // a DF with a single column "value" of type string
          .select(from_json(col("value"),bandsSchema).as("band"))
          .selectExpr("band.id as id","band.name as name","band.hometown as hometown","band.year as year")

        val streamedGuitarristsDF = spark.readStream
          .format("socket")
          .option("host","localhost")
          .option("port",12346)
          .load()       // a DF with a single column "value" of type string
          .select(from_json(col("value"),guitarPlayers.schema).as("guitarPlayer"))
          .selectExpr("guitarPlayer.id as id","guitarPlayer.name as name","guitarPlayer.guitars as guitars","guitarPlayer.band as band")


        // join stream with a stream
        val streamedJoin = streamedBandsDF.join(streamedGuitarristsDF,streamedGuitarristsDF.col("band") === streamedBandsDF.col("id"))

    /*
        - inner joins are supported
        - left/right outer joins ARE supported, but MUST have watermarks
        - full outer joins are NOT supported
     */
    streamedJoin.writeStream
      .format("console")
      .outputMode("append")         // only append supported for stream vs stream join
      .start()
      .awaitTermination()

    }


    def main(args: Array[String]): Unit = {
        //joinStreamWithStatic()
        joinStreamWithStream()
    }
}
