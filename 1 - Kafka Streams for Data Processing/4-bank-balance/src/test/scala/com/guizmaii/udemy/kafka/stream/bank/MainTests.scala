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
        val key_A = "key_A"
        val key_B = "key_B"
        val m_0   = Message(name = "Jules", amount = 2, time = Instant.MAX)
        val m_1   = Message(name = "Jules", amount = 6, time = Instant.MAX)
        val m_2   = Message(name = "Jules", amount = 3, time = Instant.MAX)
        val m_3   = Message(name = "Jules", amount = 8, time = Instant.MAX)

        producer.produce(sourceTopic)(key_A, m_0)
        producer.produce(sourceTopic)(key_A, m_1)
        producer.produce(sourceTopic)(key_B, m_2)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_A, m_1)

        def next() = consumer.consume(outputTopic)

        next().keyAndValue should be(key_A -> m_0.amount)
        next().keyAndValue should be(key_A -> (m_0.amount + m_1.amount))
        next().keyAndValue should be(key_B -> m_2.amount)
        next().keyAndValue should be(key_B -> (m_2.amount + m_3.amount))
        next().keyAndValue should be(key_A -> (m_0.amount + m_1.amount * 2))
        next() should be(null) // Assert that I consumed all the messages
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
        val key_A = "key_A"
        val key_B = "key_B"
        val m_0   = Message(name = "Jules", amount = 2, time = Instant.MAX)
        val m_1   = Message(name = "Jules", amount = 6, time = Instant.MAX)
        val m_2   = Message(name = "Jules", amount = 3, time = Instant.MAX)
        val m_3   = Message(name = "Jules", amount = 8, time = Instant.MAX)

        producer.produce(sourceTopic)(key_A, m_0)
        producer.produce(sourceTopic)(key_A, m_1)
        producer.produce(sourceTopic)(key_B, m_2)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_A, m_1)

        def next() = consumer.consume(outputTopic)

        next().keyAndValue should be(key_A -> 1)
        next().keyAndValue should be(key_A -> 2)
        next().keyAndValue should be(key_B -> 1)
        next().keyAndValue should be(key_B -> 2)
        next().keyAndValue should be(key_B -> 3)
        next().keyAndValue should be(key_B -> 4)
        next().keyAndValue should be(key_B -> 5)
        next().keyAndValue should be(key_A -> 3)
        next() should be(null) // Assert that I consumed all the messages
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
        val key_A = "key_A"
        val key_B = "key_B"
        val m_0   = Message(name = "Jules", amount = 2, time = Instant.MIN)
        val m_1   = Message(name = "Jules", amount = 6, time = Instant.MIN + 2.minutes)
        val m_2   = Message(name = "Jules", amount = 3, time = Instant.MIN + 4.minutes)
        val m_3   = Message(name = "Jules", amount = 8, time = Instant.MIN + 8.minutes)

        producer.produce(sourceTopic)(key_A, m_0)
        producer.produce(sourceTopic)(key_A, m_1)
        producer.produce(sourceTopic)(key_B, m_2)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_2)
        producer.produce(sourceTopic)(key_A, m_0)

        def next() = consumer.consume(outputTopic)

        next().keyAndValue should be(key_A -> m_0.time)
        next().keyAndValue should be(key_A -> m_1.time)
        next().keyAndValue should be(key_B -> m_2.time)
        next().keyAndValue should be(key_B -> m_3.time)
        next().keyAndValue should be(key_B -> m_3.time)
        next().keyAndValue should be(key_B -> m_3.time)
        next().keyAndValue should be(key_B -> m_2.time)
        next().keyAndValue should be(key_A -> m_0.time)
        next() should be(null) // Assert that I consumed all the messages
      }
    }
  }

  "Final Result Stream" - {
    val outputTopic = finalResultTopic

    def topology(builder: StreamsBuilder): IO[Topology] =
      for {
        source <- sourceStream(builder)
        sum    <- sumStream(source)
        count  <- transactionsCountStream(source)
        latest <- latestUpdateStream(source)
        _      <- finalResultStream(sum, count, latest).flatMap(_.to(outputTopic))
      } yield builder.build()

    "aggregates the results of the 3 other streams" in {
      testStream(topology) { (producer: Producer[String, Message], consumer: Consumer[String, FinalResult]) =>
        val key_A = "key_A"
        val key_B = "key_B"
        val m_0   = Message(name = "Jules", amount = 2, time = Instant.MIN)
        val m_1   = Message(name = "Jules", amount = 6, time = Instant.MIN + 2.minutes)
        val m_2   = Message(name = "Jules", amount = 3, time = Instant.MIN + 4.minutes)
        val m_3   = Message(name = "Jules", amount = 8, time = Instant.MIN + 8.minutes)

        producer.produce(sourceTopic)(key_A, m_0)
        producer.produce(sourceTopic)(key_A, m_1)
        producer.produce(sourceTopic)(key_B, m_2)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_3)
        producer.produce(sourceTopic)(key_B, m_2)
        producer.produce(sourceTopic)(key_A, m_0)

        def next() = consumer.consume(outputTopic)

        // format: off
        next().keyAndValue should be(key_A -> FinalResult(totalAmount = m_0.amount,                      transactionCount = 1, lastUpdated = m_0.time))
        next().keyAndValue should be(key_A -> FinalResult(totalAmount = m_0.amount + m_1.amount,         transactionCount = 2, lastUpdated = m_1.time))
        next().keyAndValue should be(key_B -> FinalResult(totalAmount = m_2.amount,                      transactionCount = 1, lastUpdated = m_2.time))
        next().keyAndValue should be(key_B -> FinalResult(totalAmount = m_2.amount + m_3.amount,         transactionCount = 2, lastUpdated = m_3.time))
        next().keyAndValue should be(key_B -> FinalResult(totalAmount = m_2.amount + m_3.amount * 2,     transactionCount = 3, lastUpdated = m_3.time))
        next().keyAndValue should be(key_B -> FinalResult(totalAmount = m_2.amount + m_3.amount * 3,     transactionCount = 4, lastUpdated = m_3.time))
        next().keyAndValue should be(key_B -> FinalResult(totalAmount = m_2.amount * 2 + m_3.amount * 3, transactionCount = 5, lastUpdated = m_2.time))
        next().keyAndValue should be(key_A -> FinalResult(totalAmount = m_0.amount * 2 + m_1.amount,     transactionCount = 3, lastUpdated = m_0.time))
        next()             should be(null) // Assert that I consumed all the messages
        // format: on
      }
    }
  }

}
