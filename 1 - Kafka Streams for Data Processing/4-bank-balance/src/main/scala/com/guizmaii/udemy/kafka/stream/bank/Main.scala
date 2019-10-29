package com.guizmaii.udemy.kafka.stream.bank

import java.time.{ Duration, Instant }
import java.util.Properties

import cats.effect.{ ExitCode, IO, IOApp, Resource }
import com.banno.kafka.producer.ProducerApi
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig, Topology }

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

object Main extends IOApp {

  /**
   * Program variables
   */
  val messagesPerSecond    = 3
  val maxAmount            = 5
  val produceMessagesEvery = 5 seconds

  import cats.implicits._
  import com.banno.kafka._
  import com.banno.kafka.admin._
  import io.circe.generic.auto._
  import retry.CatsEffect._
  import utils.RetryOps._

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

  val sourceTopic            = new NewTopic("bank-balance-source-topic", 1, 1)
  val sumTopic               = new NewTopic("bank-balance-sum-topic", 1, 1)
  val transactionsCountTopic = new NewTopic("bank-balance-transactions-count-topic", 1, 1)
  val latestUpdateTopic      = new NewTopic("bank-balance-latest-update-topic", 1, 1)
  val bootstrapServers       = BootstrapServers("localhost:9092")

  import com.goyeau.kafka.streams.circe.CirceSerdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  val producerR: Resource[IO, ProducerApi[IO, String, Message]] =
    ProducerApi
      .resource[IO, String, Message](
        bootstrapServers,
        ClientId("bank-balance-producer"),
        EnableIdempotence(true)
      )

  def generateNProducerRecords(n: Int)(maxAmount: Int): IO[List[ProducerRecord[String, Message]]] =
    for {
      messages <- List.fill(n)(newMessage(maxAmount)).sequence
    } yield messages.map(m => new ProducerRecord(sourceTopic.name, m.name, m))

  val config = {
    val c = new Properties
    c.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-stream")
    c.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.bs)
    c.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    c.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    c
  }

  def sourceStream(builder: StreamsBuilder): IO[KStream[String, Message]] =
    IO.delay(builder.stream[String, Message](sourceTopic.name))

  def sumStream(source: KStream[String, Message]): IO[KStream[String, Long]] =
    IO.delay {
      source.groupByKey
        .aggregate(0L)((_, m, acc) => acc + m.amount)
        .toStream
    }

  def transactionsCountStream(source: KStream[String, Message]): IO[KStream[String, Long]] =
    IO.delay { source.groupByKey.count().toStream }

  def latestUpdateStream(source: KStream[String, Message]): IO[KStream[String, Instant]] =
    IO.delay {
      source.groupByKey
        .aggregate(Instant.MIN) { (_, m, acc) =>
          acc.compareTo(m.time) match {
            case 0          => acc
            case i if i < 0 => m.time
            case i if i > 0 => acc
          }
        }
        .toStream
    }

  def kafkaStreamR(topology: Topology, props: Properties): Resource[IO, KafkaStreams] =
    Resource.make { IO.delay(new KafkaStreams(topology, props)) } { streams =>
      IO.delay(streams.close(Duration.ofSeconds(10))).void
    }

  def startStreams(streams: KafkaStreams): IO[Nothing] =
    IO.delay(streams.cleanUp()) *> IO.delay(streams.start()) *> IO.never

  override def run(args: List[String]): IO[ExitCode] = {
    val program =
      for {
        implicit0(logger: SelfAwareStructuredLogger[IO]) <- Slf4jLogger.create[IO]
        topics                                           = List(sourceTopic, sumTopic, transactionsCountTopic, latestUpdateTopic)
        _                                                <- AdminApi.createTopicsIdempotent[IO](bootstrapServers.bs, topics)
        builder                                          = new StreamsBuilder
        source                                           <- sourceStream(builder)
        _                                                <- sumStream(source).map(_.to(sumTopic.name))
        _                                                <- transactionsCountStream(source).map(_.to(transactionsCountTopic.name))
        _                                                <- latestUpdateStream(source).map(_.to(latestUpdateTopic.name))
        stream                                           <- kafkaStreamR(builder.build(), config).use(startStreams).start
        producer <- producerR.use { producer =>
                     val produceRecords =
                       for {
                         records <- generateNProducerRecords(messagesPerSecond)(maxAmount)
                         res     <- records.traverse(producer.sendAsync)
                       } yield res

                     produceRecords.repeatForeverEvery(produceMessagesEvery)
                   }.start
        _ <- stream.join <*> producer.join
      } yield "Done"

    program.as(ExitCode.Success)
  }
}
