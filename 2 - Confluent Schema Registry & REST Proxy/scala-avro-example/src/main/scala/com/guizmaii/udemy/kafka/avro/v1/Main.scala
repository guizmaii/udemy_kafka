package com.guizmaii.udemy.kafka.avro.v1

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.sksamuel.avro4s.{SchemaFor, ToRecord}
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord

object Main extends IOApp {

  import cats.implicits._
  import com.banno.kafka._
  import com.banno.kafka.admin._
  import com.banno.kafka.producer._
  import com.banno.kafka.schemaregistry._

  implicit def asProducerRecord[K, V](t: (NewTopic, K, V)): ProducerRecord[K, V] =
    new ProducerRecord[K, V](t._1.name, t._2, t._3)

  final case class CustomerId(id: String)
  object CustomerId {
    implicit final val record: ToRecord[CustomerId] = ToRecord[CustomerId]
  }

  final case class Customer(firstName: String, lastName: String, age: Int, height: Float)
  object Customer {
    implicit final val record: ToRecord[Customer] = ToRecord[Customer]
  }

  val sourceTopic      = new NewTopic("customer-avro-topic", 1, 1)
  val bootstrapServers = BootstrapServers("localhost:9092")
  val registry         = SchemaRegistryUrl("http://localhost:8081")

  val producerR: Resource[IO, ProducerApi[IO, CustomerId, Customer]] =
    ProducerApi.Avro4s
      .resource[IO, CustomerId, Customer](
        bootstrapServers,
        ClientId("customer-avro-producer"),
        EnableIdempotence(true),
        registry
      )

  def registerSchema[K: SchemaFor, V: SchemaFor](registryUrl: SchemaRegistryUrl, topic: NewTopic): IO[(Int, Int)] =
    SchemaRegistryApi.register[IO, K, V](registryUrl.url, topic.name)

  override def run(args: List[String]): IO[ExitCode] = {
    val program =
      for {
        implicit0(logger: SelfAwareStructuredLogger[IO]) <- Slf4jLogger.create[IO]
        topics                                           = List(sourceTopic)
        _                                                <- AdminApi.createTopicsIdempotent[IO](bootstrapServers.bs, topics)
        _                                                <- registerSchema[CustomerId, Customer](registry, sourceTopic)
        _ <- producerR.use { producer =>
              producer.sendAsync(
                (
                  sourceTopic,
                  CustomerId("Jules"),
                  Customer(firstName = "Jules", lastName = "Ivanic", age = 31, height = 176.2f)
                )
              )
            }
      } yield "Done"

    program.as(ExitCode.Success)
  }
}
