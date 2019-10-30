package com.guizmaii.udemy.kafka.stream.bank.utils

import java.nio.charset.StandardCharsets

import io.circe.parser._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

/**
 * Comes from here: https://github.com/joan38/kafka-streams-circe
 *
 * TODO: Remove this code when this PR is merged: https://github.com/joan38/kafka-streams-circe/pull/2
 */
trait CirceSerdes {
  import io.circe.syntax._

  implicit final def serializer[T: Encoder]: Serializer[T] =
    (_, t: T) => t.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)

  implicit final def deserializer[T: Decoder]: Deserializer[T] =
    (_, data: Array[Byte]) =>
      if (data eq null) null.asInstanceOf[T]
      else
        decode[T](new String(data, StandardCharsets.UTF_8))
          .fold(error => throw new SerializationException(error), identity)

  implicit final def serde[CC: Encoder: Decoder]: Serde[CC] = Serdes.serdeFrom(serializer, deserializer)
}

object KafkaSerdesWithCirceSerdes extends Serdes with CirceSerdes