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
import org.apache.kafka.streams.scala.kstream.{ KStream, KTable, Materialized }
import org.apache.kafka.streams.scala.{ ByteArrayKeyValueStore, StreamsBuilder }
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
  import utils.KTableOps._
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
  final case class FinalResult(totalAmount: Long, transactionCount: Long, lastUpdated: Instant)

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
  val finalResultTopic       = new NewTopic("bank-balance-final-result-topic", 1, 1)
  val bootstrapServers       = BootstrapServers("localhost:9092")

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import utils.KafkaSerdesWithCirceSerdes._

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
    IO.delay { builder.stream[String, Message](sourceTopic.name) }

  def sumStream(source: KStream[String, Message]): IO[KTable[String, Long]] =
    IO.delay { source.groupByKey.aggregate(0L)((_, m, acc) => acc + m.amount) }

  def transactionsCountStream(source: KStream[String, Message]): IO[KTable[String, Long]] =
    IO.delay { source.groupByKey.count() }

  /**
   * The specification of this stream in the course is unclear.
   *
   * I chose to interpret it as: keep the latest transaction timestamp
   */
  def latestUpdateStream(source: KStream[String, Message]): IO[KTable[String, Instant]] =
    IO.delay { source.groupByKey.aggregate(null.asInstanceOf[Instant])((_, m, _) => m.time) }

  /**
   * I don't understand yet why but if I don't use these `Materialized.as[...]("...")` in the `.join` calls,
   * when I produce N messages (with distinct keys, to be precise) in the "source" stream, then the "final results" stream will contains N * 3 messages.
   *
   * So, to deduplicate these N * 3 messages, I found this `Materialized.as[...]("...")` solution by reading the Kafka tests here:
   *
   *   https://github.com/confluentinc/kafka-streams-examples/blob/aefdc2fa3fe820709b069685021728e7775b1788/src/test/java/io/confluent/examples/streams/TableToTableJoinIntegrationTest.java
   *
   * And, I don't know why but I need to explicitly type these `Materialized.as[...]("...")` calls, which is annoying and ugly.
   */
  def finalResultStream(
    sumStream: KTable[String, Long],
    transactionsCountStream: KTable[String, Long],
    latestUpdateStream: KTable[String, Instant]
  ): IO[KTable[String, FinalResult]] =
    IO.delay {
      sumStream
        .join(
          transactionsCountStream,
          Materialized.as[String, (Long, Long), ByteArrayKeyValueStore]("sum-count-store")
        )((sum, count) => sum -> count)
        .join(
          latestUpdateStream,
          Materialized.as[String, (Long, Long, Instant), ByteArrayKeyValueStore]("sum-count-lastUpdate-store")
        ) { case ((sum, count), lastUpdate) => (sum, count, lastUpdate) }
        .mapValues({
          case (sum, count, lastUpdate) =>
              FinalResult(
                totalAmount = sum,
                transactionCount = count,
                lastUpdated = lastUpdate
              )
          }: ((Long, Long, Instant)) => FinalResult // Apparently, I need to explicitly type the anonymous function here because, without, Scala doesn't know which overloaded method it should use. Ugly.
        )
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
        sum                                              <- sumStream(source).flatMap(_.to(sumTopic))
        count                                            <- transactionsCountStream(source).flatMap(_.to(transactionsCountTopic))
        latest                                           <- latestUpdateStream(source).flatMap(_.to(latestUpdateTopic))
        _                                                <- finalResultStream(sum, count, latest).flatMap(_.to(finalResultTopic))
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
