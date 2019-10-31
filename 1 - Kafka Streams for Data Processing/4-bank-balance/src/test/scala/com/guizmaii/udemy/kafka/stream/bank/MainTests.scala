package com.guizmaii.udemy.kafka.stream.bank

import java.time.Instant

import cats.effect.IO
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.{ FreeSpec, Matchers }

import scala.concurrent.duration._

class MainTests extends FreeSpec with Matchers {

  import Main._
  import io.circe.generic.auto._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import utils.InstantOps._
  import utils.KTableOps._
  import utils.KafkaSerdesWithCirceSerdes._
  import utils.TestUtils._

  "true" - {
    "is true" in { true should be(true) }
  }

  "Sum Stream" - {
    val outputTopic = sumTopic

    def topology(builder: StreamsBuilder): IO[Topology] =
      for {
        source <- sourceStream(builder)
        _      <- sumStream(source).flatMap(_.to(outputTopic))
      } yield builder.build()

    "sums the, grouped by key, Messages amounts" in {
      testStream(topology) { (producer: Producer[String, Message], consumer: Consumer[String, Long]) =>
        val key        = "key"
        val anotherKey = "anotherKey"
        val m_0        = Message(name = "Jules", amount = 2, time = Instant.MAX)
        val m_1        = Message(name = "Jules", amount = 6, time = Instant.MAX)
        val m_2        = Message(name = "Jules", amount = 3, time = Instant.MAX)
        val m_3        = Message(name = "Jules", amount = 8, time = Instant.MAX)

        producer.produce(sourceTopic)(key, m_0)
        producer.produce(sourceTopic)(key, m_1)
        producer.produce(sourceTopic)(anotherKey, m_2)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(key, m_1)

        consumer.consume(outputTopic).value() should be(m_0.amount)
        consumer.consume(outputTopic).value() should be(m_0.amount + m_1.amount)
        consumer.consume(outputTopic).value() should be(m_2.amount)
        consumer.consume(outputTopic).value() should be(m_2.amount + m_3.amount)
        consumer.consume(outputTopic).value() should be(m_0.amount + m_1.amount * 2)
        consumer.consume(outputTopic) should be(null) // Assert that I consumed all the messages
      }
    }
  }

  "Transaction Count Stream" - {
    val outputTopic = transactionsCountTopic

    def topology(builder: StreamsBuilder): IO[Topology] =
      for {
        source <- sourceStream(builder)
        _      <- transactionsCountStream(source).flatMap(_.to(outputTopic))
      } yield builder.build()

    "counts the number of transactions, grouped by key" in {
      testStream(topology) { (producer: Producer[String, Message], consumer: Consumer[String, Long]) =>
        val key        = "key"
        val anotherKey = "anotherKey"
        val m_0        = Message(name = "Jules", amount = 2, time = Instant.MAX)
        val m_1        = Message(name = "Jules", amount = 6, time = Instant.MAX)
        val m_2        = Message(name = "Jules", amount = 3, time = Instant.MAX)
        val m_3        = Message(name = "Jules", amount = 8, time = Instant.MAX)

        producer.produce(sourceTopic)(key, m_0)
        producer.produce(sourceTopic)(key, m_1)
        producer.produce(sourceTopic)(anotherKey, m_2)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(key, m_1)

        consumer.consume(outputTopic).value() should be(1)
        consumer.consume(outputTopic).value() should be(2)
        consumer.consume(outputTopic).value() should be(1)
        consumer.consume(outputTopic).value() should be(2)
        consumer.consume(outputTopic).value() should be(3)
        consumer.consume(outputTopic).value() should be(4)
        consumer.consume(outputTopic).value() should be(5)
        consumer.consume(outputTopic).value() should be(3)
        consumer.consume(outputTopic) should be(null) // Assert that I consumed all the messages
      }
    }
  }

  "Latest Update Stream" - {
    val outputTopic = latestUpdateTopic

    def topology(builder: StreamsBuilder): IO[Topology] =
      for {
        source <- sourceStream(builder)
        _      <- latestUpdateStream(source).flatMap(_.to(outputTopic))
      } yield builder.build()

    "keep the latest transaction timestamp for a given key" in {
      testStream(topology) { (producer: Producer[String, Message], consumer: Consumer[String, Instant]) =>
        val key        = "key"
        val anotherKey = "anotherKey"
        val m_0        = Message(name = "Jules", amount = 2, time = Instant.MIN)
        val m_1        = Message(name = "Jules", amount = 6, time = Instant.MIN + 2.minutes)
        val m_2        = Message(name = "Jules", amount = 3, time = Instant.MIN + 4.minutes)
        val m_3        = Message(name = "Jules", amount = 8, time = Instant.MIN + 8.minutes)

        producer.produce(sourceTopic)(key, m_0)
        producer.produce(sourceTopic)(key, m_1)
        producer.produce(sourceTopic)(anotherKey, m_2)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(anotherKey, m_3)
        producer.produce(sourceTopic)(anotherKey, m_2)
        producer.produce(sourceTopic)(key, m_0)

        consumer.consume(outputTopic).value() should be(m_0.time)
        consumer.consume(outputTopic).value() should be(m_1.time)
        consumer.consume(outputTopic).value() should be(m_2.time)
        consumer.consume(outputTopic).value() should be(m_3.time)
        consumer.consume(outputTopic).value() should be(m_3.time)
        consumer.consume(outputTopic).value() should be(m_3.time)
        consumer.consume(outputTopic).value() should be(m_2.time)
        consumer.consume(outputTopic).value() should be(m_0.time)
        consumer.consume(outputTopic) should be(null) // Assert that I consumed all the messages
      }
    }
  }

}
