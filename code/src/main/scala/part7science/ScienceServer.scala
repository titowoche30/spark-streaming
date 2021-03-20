package part7science

import java.util.Properties

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.io.Source

object ScienceServer {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val kafkaTopic = "science"
    val kafkaBootsrapServer = "localhost:9092"

    // Get the producer which was already created by the kafka-topics.sh
    def getProducer() = {
        val props = new Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootsrapServer)
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"MyKafkaProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer])

        new KafkaProducer[Long, String](props)
    }

    def getRoute(producer: KafkaProducer[Long,String]): Route = {
        pathEndOrSingleSlash {
            get {
                complete(
                    HttpEntity(
                        ContentTypes.`text/html(UTF-8)`,
                        Source.fromFile("src/main/html/whackamole.html").getLines().mkString("")
                    )
                )
            }
        } ~              //otherwise
        path("api" / "report") {
            (parameter("sessionId".as[String]) & parameter("time".as[Long])) { (sessionId: String, time: Long) =>
                println(s"I've found session Id $sessionId and time = $time")

                // create a record to send to kafka
                val record = new ProducerRecord[Long,String](kafkaTopic,0,s"$sessionId,$time")
                producer.send(record)               // sending to Kafka
                producer.flush()
                complete(StatusCodes.OK)    //http 200
            }
        }
    }

    def main(args: Array[String]): Unit = {
        import scala.concurrent.ExecutionContext.Implicits.global

        // spinning up the server
        val kafkaProducer = getProducer()
        val bindingFuture = Http().bindAndHandle(getRoute(kafkaProducer), "localhost",9988)

        // cleanup
        bindingFuture.foreach { binding =>
            binding.whenTerminated.onComplete(_ => kafkaProducer.close())
        }
    }


}
