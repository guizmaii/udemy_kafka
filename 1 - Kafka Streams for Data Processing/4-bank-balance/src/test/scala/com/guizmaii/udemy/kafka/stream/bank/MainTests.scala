package com.guizmaii.udemy.kafka.stream.bank

import java.time.Instant

import cats.effect.IO
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.{ FreeSpec, Matchers }

class MainTests extends FreeSpec with Matchers with EmbeddedKafkaStreamsAllInOne {

  def runStreams[V](topics: List[NewTopic], topology: Topology)(block: => V): V =
    runStreams(topics.map(_.name), topology)(block)

  def publishToKafka[K: Serializer, V: Serializer](topic: NewTopic, key: K, message: V): Unit =
    publishToKafka(topic.name, key, message)

  import Main._
  import io.circe.generic.auto._
  import net.manub.embeddedkafka.Codecs._
  import net.manub.embeddedkafka.ConsumerExtensions._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  import utils.CirceSerdes._

  /**
   * TODO: Why this is not in the `embeddedkafka` lib?
   */
  implicit def valueCrDecoder[A]: ConsumerRecord[String, A] => (String, A) = record => record.key() -> record.value()

  "true" - {
    "is true" in { true should be(true) }
  }

  "Sum Stream" - {
    "sums the, grouped by key, Message amounts" in {
      val builder = new StreamsBuilder

      val resultTopic = "sum-result-topic"
      val source      = sourceStream(builder)
      val sum =
        source
          .flatMap(sumStream)
          .map(_.toStream.peek((k, v) => println(s"============================ $k -> $v")).to(resultTopic))

      val tests = IO.delay {
        runStreams(List(sourceTopic.name, resultTopic), builder.build()) {

          val jules = Message(name = "Jules", amount = 1, Instant.MAX)

          publishToKafka(sourceTopic, jules.name, jules)

          withConsumer[String, String, Unit] { consumer =>
            val sumStream: Stream[(String, String)] = consumer.consumeLazily(resultTopic)

            sumStream.take(1) should not be empty
          }
        }
      }

      sum.flatMap(_ => tests).unsafeRunSync()
    }
  }

}
