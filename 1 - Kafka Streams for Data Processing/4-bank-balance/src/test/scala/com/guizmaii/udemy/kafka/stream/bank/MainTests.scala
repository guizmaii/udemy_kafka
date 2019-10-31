package com.guizmaii.udemy.kafka.stream.bank

import java.time.Instant

import cats.effect.IO
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.{ FreeSpec, Matchers }

class MainTests extends FreeSpec with Matchers {

  import Main._
  import utils.TestUtils._
  import io.circe.generic.auto._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import utils.KTableOps._
  import utils.KafkaSerdesWithCirceSerdes._

  "true" - {
    "is true" in { true should be(true) }
  }

  "Sum Stream" - {
    def topology(builder: StreamsBuilder): IO[Topology] =
      for {
        source <- sourceStream(builder)
        _      <- sumStream(source).flatMap(_.to(sumTopic))
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

        consumer.consume(sumTopic).value() should be(m_0.amount)
        consumer.consume(sumTopic).value() should be(m_0.amount + m_1.amount)
        consumer.consume(sumTopic).value() should be(m_2.amount)
        consumer.consume(sumTopic).value() should be(m_2.amount + m_3.amount)
        consumer.consume(sumTopic).value() should be(m_0.amount + m_1.amount * 2)
      }
    }
  }

  "Transaction count Stream" - {
    def topology(builder: StreamsBuilder): IO[Topology] =
      for {
        source <- sourceStream(builder)
        _      <- transactionsCountStream(source).flatMap(_.to(transactionsCountTopic))
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

        consumer.consume(transactionsCountTopic).value() should be(1)
        consumer.consume(transactionsCountTopic).value() should be(2)
        consumer.consume(transactionsCountTopic).value() should be(1)
        consumer.consume(transactionsCountTopic).value() should be(2)
        consumer.consume(transactionsCountTopic).value() should be(3)
        consumer.consume(transactionsCountTopic).value() should be(4)
        consumer.consume(transactionsCountTopic).value() should be(5)
        consumer.consume(transactionsCountTopic).value() should be(3)
      }
    }
  }

}
