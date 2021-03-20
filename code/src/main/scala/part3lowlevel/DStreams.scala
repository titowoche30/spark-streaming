package part3lowlevel

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Locale

import common.Stock
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {
    val spark = SparkSession.builder()
      .appName("DStreams")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //DStreams are never-ending sequence of RDDs, available under a StreamingContext

    /*
        Spark Streaming Context = entry point to the DStreams API
            - needs the spark context
            - a duration = batch interval
     */

    val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

    /*
        - Define input sources by creating DStreams
        - Define transformations on DStreams
        - call an action on DStreams
        - starts ALL computations with ssc.start()
            - after that, no more computations can be added
        - await termination, or stop the computation
            - after that, you cannot restart the ssc
     */

    def readFromSocket() = {
        // reading a text DStream from a socket
        val socketStream: DStream[String] = ssc.socketTextStream("localhost",12345)

        // transformation = lazy
        val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

        // action
        //wordsStream.print()
        wordsStream.saveAsTextFiles("src/main/resources/data/words/")  // each folder = RDD = batch, each file = a partition of the RDD

        // actually trigger the computation(action)
        ssc.start()
        ssc.awaitTermination()
    }

    def createNewFile() = {
        new Thread( () => {
            Thread.sleep(5000)

            val path = "src/main/resources/data/stocks"
            val dir = new File(path)
            val nFiles = dir.listFiles().length
            val newFile = new File(s"$path/newStocks$nFiles.csv")
            newFile.createNewFile()

            val writer = new FileWriter(newFile)
            writer.write(
                """
                  |AAPL,Sep 1 2000,12.88
                  |AAPL,Oct 1 2000,9.78
                  |AAPL,Nov 1 2000,8.25
                  |AAPL,Dec 1 2000,7.44
                  |AAPL,Jan 1 2001,10.81
                  |AAPL,Feb 1 2001,9.12
                  """.stripMargin.trim
            )

            writer.close()
        }).start()
    }


    def readFromFile() = {
        createNewFile() // assynchonous, e. g., operates on another thread
        val stocksFilePath = "src/main/resources/data/stocks"

        /*
            ssc.textFileStream monitors a directory for NEW FILES
         */

        val textStream: DStream[String] = ssc.textFileStream(stocksFilePath)

        // transformations
        val dateFormat = new SimpleDateFormat("MMM d yyyy",Locale.ENGLISH)
        val stocksStream: DStream[Stock] = textStream.map { line =>
            val tokens = line.split(",")
            val company = tokens(0)
            val date = new Date(dateFormat.parse(tokens(1)).getTime)
            val price = tokens(2).toDouble

            Stock(company,date,price)
        }
        // action
        stocksStream.print()

        // start the computations
        ssc.start()
        ssc.awaitTermination()


    }


    def main(args: Array[String]): Unit = {
         readFromSocket()
        //readFromFile()
    }



}
