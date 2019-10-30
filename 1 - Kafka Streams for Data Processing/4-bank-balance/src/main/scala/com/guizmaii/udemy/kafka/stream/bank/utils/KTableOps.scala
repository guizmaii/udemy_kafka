package com.guizmaii.udemy.kafka.stream.bank.utils

import cats.effect.IO
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.scala.kstream.KTable

object KTableOps {

  import cats.implicits._

  implicit final class KTableOps[K, V](private val ktable: KTable[K, V]) extends AnyVal {
    def to(topic: NewTopic)(implicit P: Produced[K, V]): IO[KTable[K, V]] =
      IO.delay { ktable.toStream.to(topic.name) } *> ktable.pure[IO]
  }

}
