package com.guizmaii.udemy.kafka.stream.colour

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object FavouriteColour extends App {

  val config = new Properties
  config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, s"favourite-colour-app")
  config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  val builder = new StreamsBuilder

  builder
    .stream[String, String](s"favourite-colour-input")
    .filter((_, colour) => "green" == colour || "red" == colour || "blue" == colour)
    .selectKey((user, _) => user)
    .to(s"favourite-color-state")

  builder
    .table[String, String](s"user-keys-and-colours")
      .groupBy((_, colour) => (colour, colour))
      .count()
      .toStream
      .to(s"favourite-colour-output")

  val streams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()
  streams.start()

  sys.addShutdownHook {
    val _ = streams.close(Duration.ofSeconds(10))
  }

}
