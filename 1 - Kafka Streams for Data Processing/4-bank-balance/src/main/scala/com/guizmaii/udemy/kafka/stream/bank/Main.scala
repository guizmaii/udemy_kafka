package com.guizmaii.udemy.kafka.stream.bank

import java.time.{ Duration, Instant }
import java.util.Properties

import cats.effect.{ ContextShift, IO, Resource, Timer }
import com.banno.kafka.producer.ProducerApi
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }

import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

object Main extends App {

  import cats.implicits._
  import com.banno.kafka._
  import com.banno.kafka.admin._
  import io.circe.generic.auto._
  import io.circe.syntax._
  import retry.CatsEffect._
  import utils.BetterRetry._

  implicit val timer: Timer[IO]      = IO.timer(global)
  implicit val cxt: ContextShift[IO] = IO.contextShift(global)

  val customers = List(
    "John",
    "Jules",
    "Thomas",
    "Alice",
    "Kayla",
    "Robert"
  )

  final case class Message(name: String, amount: Int, time: Instant)

  def newMessage(maxAmount: Int) =
    IO.delay {
      Message(
        name = customers(Random.nextInt(customers.length)),
        amount = Random.nextInt(maxAmount),
        time = Instant.now()
      )
    }

  val sourceTopic           = new NewTopic("bank-balance-source-topic-0", 1, 1)
  val sumTopic              = new NewTopic("bank-balance-sum-topic-0", 1, 1)
  val maxTopic              = new NewTopic("bank-balance-max-topic-0", 1, 1)
  val kafkaBootstrapServers = BootstrapServers("localhost:9092")

  val producerR: Resource[IO, ProducerApi[IO, String, String]] =
    ProducerApi
      .resource[IO, String, String](
        kafkaBootstrapServers,
        ClientId("bank-balance-producer")
      )

  def produceNMessages(n: Int)(maxAmount: Int)(implicit p: ProducerApi[IO, String, String]): IO[List[RecordMetadata]] =
    for {
      messages <- List.fill(n)(newMessage(maxAmount)).sequence
      records = messages.map { m =>
        new ProducerRecord(sourceTopic.name, m.asJson.noSpaces): ProducerRecord[String, String]
      }
      res <- records.traverse(p.sendAsync)
    } yield res

  val config = {
    val c = new Properties
    c.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-stream-0")
    c.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers.bs)
    c.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    c.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    c
  }

  import com.goyeau.kafka.streams.circe.CirceSerdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  def sumStream(builder: StreamsBuilder): IO[Unit] =
    IO.delay {
      builder
        .stream[String, Message](sourceTopic.name)
        .selectKey((_, m) => m.name)
        .groupByKey
        .aggregate(0L)((_, m, acc) => acc + m.amount)
        .toStream
        .to(sumTopic.name)
    }

  def kafkaStreamR(topology: Topology, props: Properties): Resource[IO, KafkaStreams] =
    Resource.make { IO.delay(new KafkaStreams(topology, props)) } { streams =>
      IO.delay(streams.close(Duration.ofSeconds(10))).void
    }

  def startStreams(streams: KafkaStreams): IO[Nothing] =
    IO.delay(streams.cleanUp()) *> IO.delay(streams.start()) *> IO.never

  val program =
    for {
      implicit0(logger: SelfAwareStructuredLogger[IO]) <- Slf4jLogger.create[IO]
      _                                                <- AdminApi.createTopicsIdempotent[IO](kafkaBootstrapServers.bs, sourceTopic :: sumTopic :: maxTopic :: Nil)
      builder                                          = new StreamsBuilder
      _                                                <- sumStream(builder)
      stream                                           <- kafkaStreamR(builder.build(), config).use(startStreams).start
      producer                                         <- producerR.use(implicit p => produceNMessages(n = 3)(maxAmount = 5).retryForeverEvery(30 second)).start
      _                                                <- stream.join <*> producer.join
    } yield "Done"

  program.unsafeRunSync()
}
