package com.guizmaii.udemy.kafka.stream.colour

import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{ KafkaStreams, StreamsConfig }

object FavouriteColour extends App {

  val config = new Properties
  config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-app")
  config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  val builder = new StreamsBuilder

  val inputTopic        = "favourite-colour-input"
  val resultTopic       = "favourite-colour-output"

  builder
    .table[String, String](inputTopic)
    .filter((_, colour) => "green" == colour || "red" == colour || "blue" == colour)
    .groupBy((_, colour) => (colour, colour))
    .count()
    .toStream
    .to(resultTopic)

  val streams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()
  streams.start()

  sys.addShutdownHook {
    val _ = streams.close(Duration.ofSeconds(10))
  }

}
