package com.guizmaii.udemy.kafka.stream.bank

import java.time.Instant
import java.util.Properties

import cats.effect.{IO, Resource, Timer}
import com.banno.kafka.producer.ProducerApi
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.apache.kafka.streams.StreamsConfig

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

object Main extends App {

  implicit val timer: Timer[IO] = IO.timer(global)

  val customers = List(
    "John",
    "Jules",
    "Thomas",
    "Alice",
    "Kayla",
    "Robert"
  )

  def unsafeMessage =
    s"""
       |{
       |  "name": ${customers(Random.nextInt(customers.length))},
       |  "amount": ${Random.nextInt(Int.MaxValue)},
       |  "time": ${Instant.now()}
       |}
       |""".stripMargin

  val config = new Properties
  config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app")
  config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

  import cats.implicits._
  import com.banno.kafka._
  import com.banno.kafka.admin._
  import org.apache.kafka.clients.admin.NewTopic
  import retry.CatsEffect._
  import utils.BetterRetry._

  val sourceTopic: NewTopic = new NewTopic("bank-balance-source-topic", 1, 1)
  val kafkaBootstrapServers = "localhost:9092"

  val producer: Resource[IO, ProducerApi[IO, String, String]] =
    ProducerApi
      .resource[IO, String, String](
        BootstrapServers(kafkaBootstrapServers),
        ClientId("bank-balance-producer")
      )

  def produceOneHundredMessage(p: ProducerApi[IO, String, String]): IO[List[RecordMetadata]] =
    List
      .fill(100)(new ProducerRecord(sourceTopic.name, unsafeMessage): ProducerRecord[String, String])
      .traverse(p.sendAsync)

  val program =
    for {
      logger <- Slf4jLogger.create[IO]
      _ <- AdminApi.createTopicsIdempotent[IO](kafkaBootstrapServers, sourceTopic :: Nil)
      r <- producer.use(p => retryForeverEvery(produceOneHundredMessage(p), 3 second))
    } yield r

  program.unsafeRunSync()
}
