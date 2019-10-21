package com.guizmaii.udemy.kafka.beginnercourse.tutorial1

import java.util.{ Collections, Properties }

import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import scala.concurrent.duration._

object ConsumerDemo extends App {

  import scala.compat.java8.DurationConverters._
  import scala.jdk.CollectionConverters._

  val logger = LoggerFactory.getLogger(ConsumerDemo.getClass)

  val props = new Properties()
  props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-first-consumer-demo")
  props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(Collections.singleton("first_topic"))

  while (true) {
    val records = consumer.poll(100.millis.toJava)

    for { r <- records.asScala } {
      logger.info(s"""
                     |Key:        ${r.key}
                     |Value:      ${r.value}
                     |Partition:  ${r.partition}
                     |Offset:     ${r.offset}
                     |Timestamp:  ${r.timestamp}
                     |""".stripMargin)
    }
  }

}
