package part7science

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SparkSession}

object ScienceSparkAggregator {
    val spark: SparkSession = SparkSession.builder()
      .appName("The Science Project")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    case class UserResponse(sessionId: String, clickDuration:Long)
    case class UserAvgResponse(sessionId: String, avgDuration: Double)

    def readUserResponses(): Dataset[UserResponse] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","science")
      .load()
      .select("value")
      .as[String]
      .map { line =>
          val tokens = line.split(",")
          val sessionId = tokens(0)
          val time = tokens(1).toLong

          UserResponse(sessionId,time)
      }

    // aggregate the rolling average response time over the past 10 clicks

    // ex: rolling sum of numbers
    // 1 2 3 4 5 6 7 8 9 10
    // 6 9 12 15 18 21 24 27

    // ex dos clicks pra 3 clicks atrás

    // session  - tempos
    // session - [100,200,300,400,500,600]

    // updateUserResponseTime("abc",[100,200,300,400,500,600],Empty) -----> input
    // 100 -> state becomes [100]
    // 200 -> state becomes [100,200]
    // 300 -> state becomes [100,200,300]
    // 400 -> state becomes [200,300,400]
    // 500 -> state becomes [300,400,500]
    // 600 -> state becomes [400,500,600]

    // Iterator will contain 200,300,400,500
    // It will run once per batch per key

    def updateUserResponseTime
    (n: Int)
    (sessionId: String, group: Iterator[UserResponse], state: GroupState[List[UserResponse]])
    :Iterator[UserAvgResponse] = {
        group.flatMap { record =>
            val lastWindow =
                if (state.exists) state.get
                else List()

            val windowLength = lastWindow.length
            val newWindow =
                if (windowLength >= n) lastWindow.tail :+ record
                else lastWindow :+ record

            // for Spark to give us acess to the state in the next batch
            state.update(newWindow)

            if (newWindow.length >= n) {
                val newAverage = newWindow.map(_.clickDuration).sum * 1.0 / n
                Iterator(UserAvgResponse(sessionId,newAverage))
            } else {
                Iterator()
            }
        }
    }



    def getAverageResponseTime(n: Int) = {
        readUserResponses()
          .groupByKey(_.sessionId)
          .flatMapGroupsWithState(OutputMode.Append,GroupStateTimeout.NoTimeout())(updateUserResponseTime(n))
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()
    }

    def logUserResponses() = {
        readUserResponses().writeStream
          .format("console")
          .outputMode("append")
          .start()
          .awaitTermination()

    }


    def main(args: Array[String]): Unit = {
        getAverageResponseTime(3)
        //logUserResponses()
    }
}
