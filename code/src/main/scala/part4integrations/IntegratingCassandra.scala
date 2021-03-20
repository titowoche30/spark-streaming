package part4integrations

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._
import common._

object IntegratingCassandra {
    val spark: SparkSession = SparkSession.builder()
      .appName("Integrating Cassandra")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    def writeStreamToCassandraInBatches() = {
        val carsDS = spark.readStream
          .schema(carsSchema)
          .json("src/main/resources/data/cars")
          .as[Car]

        // Antes tem que criar o keyspace e a table dentro do cassandra
        carsDS
          .writeStream
          .foreachBatch { (batch: Dataset[Car], batchId: Long ) =>
            // save this batch to cassandra in a single table write
              batch
                .select("Name","Horsepower")
                .write
                .cassandraFormat("cars","public")       //type enrichment
                .mode(SaveMode.Append)
                .save()
          }
          .start()
          .awaitTermination()
    }


    // Se fosse um DF em vez de DS seria ForeachWriter[Row]
    class CarCassandraForeachWriter extends ForeachWriter[Car] {
        /*
            - on every batch, on every partition 'partitionId'
                -on every "epoch" = chunk of data
                    - call the open method; if false, skip this chunk
                    - for each entry in this chunk, call the process method
                    - call the close method eiter at the end of the chunk or with an error if it was thrown
         */

        val keyspace = "public"
        val table = "cars"
        val connector = CassandraConnector(spark.sparkContext.getConf)

        override def open(partitionId: Long, epochId: Long): Boolean = {
            println("Open connection")
            true
        }

        override def process(car: Car): Unit = {
            connector.withSessionDo { session =>
                session.execute(
                    s"""
                       |insert into $keyspace.$table("Name","Horsepower")
                       |values ('${car.Name}',${car.Horsepower.orNull})
                       |""".stripMargin
                )

            }


        }

        override def close(errorOrNull: Throwable): Unit = println("Closing Connection")

    }


    def writeStreamToCassandra() = {
        val carsDS = spark.readStream
          .schema(carsSchema)
          .json("src/main/resources/data/cars")
          .as[Car]

        // Antes tem que criar o keyspace e a table dentro do cassandra
        // Esse foreach serve pra qualquer outra integração com spark


        carsDS
          .writeStream
          .foreach(new CarCassandraForeachWriter)
          .start()
          .awaitTermination()
    }


    def main(args: Array[String]): Unit = {
        //writeStreamToCassandraInBatches()
        writeStreamToCassandra()
    }


}
