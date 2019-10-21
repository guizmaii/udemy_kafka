package com.guizmaii.udemy.kafka.beginnercourse.realapp

import org.slf4j.LoggerFactory
import pureconfig._
import pureconfig.generic.auto._

case class TwitterConfig(apiKey: String, apiSecret: String, accessToken: String, accessSecret: String)

object Main extends App {

  val logger = LoggerFactory.getLogger(Main.getClass)

  ConfigSource.default.load[TwitterConfig] match {
    case Left(error)   => logger.error(s"Configuration error: $error")
    case Right(config) => new TwitterProducer(config).run()
  }

}
