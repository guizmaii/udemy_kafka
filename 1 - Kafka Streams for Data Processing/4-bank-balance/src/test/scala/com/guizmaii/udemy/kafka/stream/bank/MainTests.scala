package com.guizmaii.udemy.kafka.stream.bank

import java.time.Instant

import cats.effect.{ IO, Resource }
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{ Topology, TopologyTestDriver }
import org.scalatest.{ FreeSpec, Matchers }

object Helpers {
  implicit final class ConsumerRecordFactoryOps[K, V](private val factory: ConsumerRecordFactory[K, V]) extends AnyVal {
    def make(k: K, v: V): ConsumerRecord[Array[Byte], Array[Byte]] = factory.create(k, v)
  }

  implicit final class TopologyTestDriverOps(private val topologyTestDriver: TopologyTestDriver) extends AnyVal {
    def read[K: Deserializer, V: Deserializer](topic: NewTopic): ProducerRecord[K, V] =
      topologyTestDriver.readOutput(topic.name, implicitly[Deserializer[K]], implicitly[Deserializer[V]])
  }
}

class MainTests extends FreeSpec with Matchers {

  import Helpers._
  import Main._
  import io.circe.generic.auto._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import utils.KTableOps._
  import utils.KafkaSerdesWithCirceSerdes._

  def ConsumerRecordFactory[K: Serializer, V: Serializer](topic: NewTopic): ConsumerRecordFactory[K, V] =
    new ConsumerRecordFactory[K, V](topic.name, implicitly[Serializer[K]], implicitly[Serializer[V]])

  def topologyTestDriverR(topology: Topology): Resource[IO, TopologyTestDriver] =
    Resource.fromAutoCloseable(IO.delay { new TopologyTestDriver(topology, config) })

  "true" - {
    "is true" in { true should be(true) }
  }

  /*

    - 1 input topic
    - n input key/messages

    - 1 output topic
    - n expected records

    - a topology

   */

  trait Producer[K, V] {
    def produce(k: K, v: V): Unit
  }

  trait Consumer[K, V] {
    def consume(): ProducerRecord[K, V]
  }

  def testStream[InputKey: Serializer, InputValue: Serializer, OutputKey: Deserializer, OutputValue: Deserializer, T](
    topology: Topology
  )(
    inputTopic: NewTopic,
    outputTopic: NewTopic
  )(test: (Producer[InputKey, InputValue], Consumer[OutputKey, OutputValue]) => T): T =
    topologyTestDriverR(topology).use { testDriver =>
      IO.delay {
        val factory = ConsumerRecordFactory[InputKey, InputValue](inputTopic)

        val producer = new Producer[InputKey, InputValue] {
          override def produce(k: InputKey, v: InputValue): Unit = testDriver.pipeInput(factory.make(k, v))
        }
        val consumer = new Consumer[OutputKey, OutputValue] {
          override def consume(): ProducerRecord[OutputKey, OutputValue] =
            testDriver.read[OutputKey, OutputValue](outputTopic)
        }

        test(producer, consumer)
      }
    }.unsafeRunSync()

  "Sum Stream" - {
    "sums the, grouped by key, Message amounts" in {
      val builder = new StreamsBuilder

      for {
        source <- sourceStream(builder)
        _      <- sumStream(source).flatMap(_.to(sumTopic))
      } yield testStream(builder.build)(sourceTopic, sumTopic) {
        (producer: Producer[String, Message], consumer: Consumer[String, Long]) =>
          val key        = "key"
          val anotherKey = "anotherKey"
          val m_0        = Message(name = "Jules", amount = 2, time = Instant.MAX)
          val m_1        = Message(name = "Jules", amount = 6, time = Instant.MAX)
          val m_2        = Message(name = "Jules", amount = 3, time = Instant.MAX)
          val m_3        = Message(name = "Jules", amount = 8, time = Instant.MAX)

          producer.produce(key, m_0)
          producer.produce(key, m_1)
          producer.produce(anotherKey, m_2)
          producer.produce(anotherKey, m_3)

          consumer.consume().value() should be(m_0.amount)
          consumer.consume().value() should be(m_0.amount + m_1.amount)
          consumer.consume().value() should be(m_2.amount)
          consumer.consume().value() should be(m_2.amount + m_3.amount)
      }
    }
  }

}
