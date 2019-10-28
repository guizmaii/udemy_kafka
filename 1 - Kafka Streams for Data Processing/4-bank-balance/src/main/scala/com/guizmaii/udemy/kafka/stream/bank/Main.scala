package com.guizmaii.udemy.kafka.stream.bank

import java.time.Instant
import java.util.Properties

import cats.effect.{ IO, Resource, Timer }
import com.banno.kafka.producer.ProducerApi
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.streams.StreamsConfig

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

object Main extends App {

  import cats.implicits._
  import com.banno.kafka._
  import com.banno.kafka.admin._
  import org.apache.kafka.clients.admin.NewTopic
  import retry.CatsEffect._
  import utils.BetterRetry._

  implicit val timer: Timer[IO] = IO.timer(global)

  val customers = List(
    "John",
    "Jules",
    "Thomas",
    "Alice",
    "Kayla",
    "Robert"
  )

  val newMessage =
    IO.delay {
      s"""
         |{
         |  "name": ${customers(Random.nextInt(customers.length))},
         |  "amount": ${Random.nextInt(Int.MaxValue)},
         |  "time": ${Instant.now()}
         |}
         |""".stripMargin
    }

  val sourceTopic           = new NewTopic("bank-balance-source-topic", 1, 1)
  val kafkaBootstrapServers = BootstrapServers("localhost:9092")

  val producer: Resource[IO, ProducerApi[IO, String, String]] =
    ProducerApi
      .resource[IO, String, String](
        kafkaBootstrapServers,
        ClientId("bank-balance-producer")
      )

  def produceOneHundredMessage(p: ProducerApi[IO, String, String]): IO[List[RecordMetadata]] =
    for {
      messages <- List.fill(100)(newMessage).sequence
      records  = messages.map(m => new ProducerRecord(sourceTopic.name, m): ProducerRecord[String, String])
      res      <- records.traverse(p.sendAsync)
    } yield res

  val program =
    for {
      logger <- Slf4jLogger.create[IO]
      _      <- AdminApi.createTopicsIdempotent[IO](kafkaBootstrapServers.bs, sourceTopic :: Nil)
      r      <- producer.use(p => retryForeverEvery(produceOneHundredMessage(p), 3 second))
    } yield r

  program.unsafeRunSync()

}
