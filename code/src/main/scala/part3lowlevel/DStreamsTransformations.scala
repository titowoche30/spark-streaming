package part3lowlevel

import java.io.File
import java.sql.Date
import java.time.{LocalDate, Period}

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import common._
import org.apache.spark.streaming.dstream.DStream

object DStreamsTransformations {
    val spark = SparkSession.builder()
      .appName("DStreams Transformations")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    import spark.implicits._        // for encoders to create DS

    //6:Chassidy:Concepcion:Bourthouloume:F:1990-11-24:954-59-9172:64652
    def readPeople() = ssc.socketTextStream("localhost",12345).map {
        line =>
            val tokens = line.split(":")
            Person(
                tokens(0).toInt,
                tokens(1),
                tokens(2),
                tokens(3),
                tokens(4),
                Date.valueOf(tokens(5)),             //retorna um Date
                tokens(6),
                tokens(7).toInt,
            )
    }

    // map, flatMap, filter
    def peopleAges():DStream[(String,Int)] = readPeople().map { person =>
        val age = Period.between(person.birthDate.toLocalDate,LocalDate.now()).getYears
        (s"${person.firstName} ${person.lastName}", age)

    }

    def peopleSmallNames():DStream[String] = readPeople().flatMap { person =>
        List(person.firstName,person.middleName)
    }

    def highIncomePeople() = readPeople().filter(_.salary > 80000)

    // count
    def countPeople() = readPeople().count() //the number of entreis in every batch

    // count by value. It operates PER BATCH
    def countNames(): DStream[(String,Long)] = readPeople().map(_.firstName).countByValue()

    /* reduce by key
      - works on DStream of tuples
      - works PER BATCH

     */
    def countNamesReduce(): DStream[(String,Int)] = readPeople()
      .map(_.firstName)
      .map(name => (name,1))
      .reduceByKey( (a,b) => a + b )

    // reduction by key takes a function that will reduce the second members of the tuples for every equal first member, e. g., whenever we get tuples with the same name, the value in the second member of the tuple will be reduce by the function provided

    // foreach RDD
    def saveToJson() = readPeople().foreachRDD { rdd =>
        val ds = spark.createDataset(rdd)       // DS per batch
        val f = new File("src/main/resources/data/people")
        val nFiles = f.listFiles().length
        val path = s"src/main/resources/data/people/people$nFiles.json"
        ds.write.json(path)

    }



    def main(args: Array[String]): Unit = {
        //val stream = readPeople()
        //val stream = peopleAges()
//        val stream = peopleSmallNames()
//        val stream = highIncomePeople()
//        val stream = countPeople()
        //val stream = countNames()
        //val stream = countNamesReduce()
       // stream.print()

        saveToJson()
        ssc.start()
        ssc.awaitTermination()
    }

}
