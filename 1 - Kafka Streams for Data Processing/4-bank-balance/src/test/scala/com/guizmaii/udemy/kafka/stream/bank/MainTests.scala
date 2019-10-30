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
import org.scalatest.{ Assertion, FreeSpec, Matchers }

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

  "Sum Stream" - {
    "sums the, grouped by key, Message amounts" in {

      def test(topology: Topology): IO[Assertion] =
        topologyTestDriverR(topology).use { testDriver =>
          IO.delay {
            val factory: ConsumerRecordFactory[String, Message] = ConsumerRecordFactory(sourceTopic)

            val key = "key"
            val anotherKey = "anotherKey"
            val m_0 = Message(name = "Jules", amount = 2, time = Instant.MAX)
            val m_1 = Message(name = "Jules", amount = 6, time = Instant.MAX)
            val m_2 = Message(name = "Jules", amount = 3, time = Instant.MAX)
            val m_3 = Message(name = "Jules", amount = 8, time = Instant.MAX)

            testDriver.pipeInput(factory.make(key, m_0))
            testDriver.pipeInput(factory.make(key, m_1))
            testDriver.pipeInput(factory.make(anotherKey, m_2))
            testDriver.pipeInput(factory.make(anotherKey, m_3))

            testDriver.read[String, Long](sumTopic).value() should be(m_0.amount)
            testDriver.read[String, Long](sumTopic).value() should be(m_0.amount + m_1.amount)
            testDriver.read[String, Long](sumTopic).value() should be(m_2.amount)
            testDriver.read[String, Long](sumTopic).value() should be(m_2.amount + m_3.amount)
          }
        }

      val builder = new StreamsBuilder

      val program =
        for {
          source <- sourceStream(builder)
          _      <- sumStream(source).flatMap(_.to(sumTopic))
          res    <- test(builder.build)
        } yield res

      program.unsafeRunSync()
    }
  }

}
