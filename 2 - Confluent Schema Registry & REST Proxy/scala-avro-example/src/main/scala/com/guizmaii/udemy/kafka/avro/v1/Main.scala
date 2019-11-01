package com.guizmaii.udemy.kafka.avro.v1

import cats.effect.{ ExitCode, IO, IOApp, Resource }
import com.banno.kafka.consumer.ConsumerApi
import com.sksamuel.avro4s.{ RecordFormat, SchemaFor }
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition

object Main extends IOApp {

  import cats.implicits._
  import com.banno.kafka._
  import com.banno.kafka.admin._
  import com.banno.kafka.producer._
  import com.banno.kafka.schemaregistry._

  import scala.concurrent.duration._

  implicit final class ProducerOps[F[_], K, V](private val producer: ProducerApi[F, K, V]) extends AnyVal {
    def sendAsync(topic: NewTopic, k: K, v: V): F[RecordMetadata] =
      producer.sendAsync(new ProducerRecord[K, V](topic.name, k, v))
  }

  final case class CustomerId(id: String)
  object CustomerId {
    implicit final val format: RecordFormat[CustomerId] = RecordFormat[CustomerId]
  }

  final case class Customer(firstName: String, lastName: String, age: Int, height: Float)
  object Customer {
    implicit final val format: RecordFormat[Customer] = RecordFormat[Customer]
  }

  val sourceTopic      = new NewTopic("customer-avro-topic", 1, 1)
  val bootstrapServers = BootstrapServers("localhost:9092")
  val registry         = SchemaRegistryUrl("http://localhost:8081")

  val producerR: Resource[IO, ProducerApi[IO, CustomerId, Customer]] =
    ProducerApi.Avro4s
      .resource[IO, CustomerId, Customer](
        bootstrapServers,
        registry,
        ClientId("customer-avro-producer"),
        EnableIdempotence(true)
      )

  val consumerR: Resource[IO, ConsumerApi[IO, CustomerId, Customer]] =
    ConsumerApi.Avro4s.resource[IO, CustomerId, Customer](
      bootstrapServers,
      registry,
      ClientId("customer-avro-consumer"),
      GroupId("customer-avro-consumer-group"),
      AutoOffsetReset.earliest
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
                sourceTopic,
                CustomerId("Jules"),
                Customer(firstName = "Jules", lastName = "Ivanic", age = 31, height = 176.2f)
              )
            }
        _ <- consumerR.use { consumer: ConsumerApi[IO, CustomerId, Customer] =>
              val initialOffsets = Map.empty[TopicPartition, Long] // Start from beginning

              consumer.assign(sourceTopic.name, initialOffsets) *>
                consumer
                  .recordStream(1.second)
                  .take(1)
                  .compile
                  .toVector
                  .map(_.foreach(println))
            }
      } yield "Done"

    program.as(ExitCode.Success)
  }
}
