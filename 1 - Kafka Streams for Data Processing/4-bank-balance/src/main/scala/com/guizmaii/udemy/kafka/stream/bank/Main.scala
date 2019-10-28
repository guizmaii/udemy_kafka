package com.guizmaii.udemy.kafka.stream.bank

import java.time.{ Duration, Instant }
import java.util.Properties

import cats.effect.{ ExitCode, IO, IOApp, Resource }
import com.banno.kafka.producer.ProducerApi
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
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

  val sourceTopic       = new NewTopic("bank-balance-source-topic", 1, 1)
  val sumTopic          = new NewTopic("bank-balance-sum-topic", 1, 1)
  val latestUpdateTopic = new NewTopic("bank-balance-latest-update-topic", 1, 1)
  val bootstrapServers  = BootstrapServers("localhost:9092")

  import com.goyeau.kafka.streams.circe.CirceSerdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  val producerR: Resource[IO, ProducerApi[IO, String, Message]] =
    ProducerApi
      .resource[IO, String, Message](
        bootstrapServers,
        ClientId("bank-balance-producer")
      )

  def produceNMessages(n: Int)(maxAmount: Int)(implicit p: ProducerApi[IO, String, Message]): IO[List[RecordMetadata]] =
    for {
      messages <- List.fill(n)(newMessage(maxAmount)).sequence
      records  = messages.map(m => new ProducerRecord(sourceTopic.name, m): ProducerRecord[String, Message]) // This ugly explicit type is required because ProducerRecord is a Java class with a Java API...
      res      <- records.traverse(p.sendAsync)
    } yield res

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
      source
        .selectKey((_, m) => m.name)
        .groupByKey
        .aggregate(0L)((_, m, acc) => acc + m.amount)
        .toStream
    }

  def latestUpdateStream(source: KStream[String, Message]): IO[KStream[String, Instant]] =
    IO.delay {
      source
        .selectKey((_, m) => m.name)
        .groupByKey
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
        _                                                <- AdminApi.createTopicsIdempotent[IO](bootstrapServers.bs, List(sourceTopic, sumTopic, latestUpdateTopic))
        builder                                          = new StreamsBuilder
        source                                           <- sourceStream(builder)
        _                                                <- sumStream(source).map(_.to(sumTopic.name))
        _                                                <- latestUpdateStream(source).map(_.to(latestUpdateTopic.name))
        stream                                           <- kafkaStreamR(builder.build(), config).use(startStreams).start
        producer <- producerR.use { implicit p =>
                     produceNMessages(messagesPerSecond)(maxAmount).repeatForeverEvery(produceMessagesEvery)
                   }.start
        _ <- stream.join <*> producer.join
      } yield "Done"

    program.as(ExitCode.Success)
  }
}
