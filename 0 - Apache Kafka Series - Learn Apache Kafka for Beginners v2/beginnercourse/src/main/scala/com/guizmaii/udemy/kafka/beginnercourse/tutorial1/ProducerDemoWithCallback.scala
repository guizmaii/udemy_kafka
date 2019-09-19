package com.guizmaii.udemy.kafka.beginnercourse.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

object ProducerDemoWithCallback extends App {

  val logger = LoggerFactory.getLogger(ProducerDemoWithCallback.getClass)

  val props = new Properties()
  props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  (0 to 10).map { i =>
    val record = new ProducerRecord[String, String]("first_topic", s"Tata $i")
    producer.send(
      record,
      (metadata: RecordMetadata, exception: Exception) =>
        Either.cond(exception == null, metadata, exception) match {
          case Left(_) => logger.error("Error while producing", exception)
          case Right(_) =>
            logger.info(
              s"""
                 |Received new metadata:
                 |  - Topic:     ${metadata.topic()}
                 |  - Partition: ${metadata.partition()}
                 |  - Offset:    ${metadata.offset()}
                 |  - Timestamp: ${metadata.timestamp()}
                 |""".stripMargin
            )
      }
    )
  }
  producer.close()
}
