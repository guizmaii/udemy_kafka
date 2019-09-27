package com.guizmaii.udemy.kafka.beginnercourse.realapp

import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit }

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{ Constants, HttpHosts }
import com.twitter.hbc.httpclient.BasicClient
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.slf4j.LoggerFactory

import scala.util.{ Failure, Success, Try }

class TwitterProducer(twitterConfig: TwitterConfig) {

  private val logger = LoggerFactory.getLogger(classOf[TwitterProducer])

  import scala.jdk.CollectionConverters._

  private val msgQueue = new LinkedBlockingQueue[String](10000)

  /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
  private val hosebirdHosts    = new HttpHosts(Constants.STREAM_HOST)
  private val hosebirdEndpoint = new StatusesFilterEndpoint

  private val terms = List("kafka")

  hosebirdEndpoint.trackTerms(terms.asJava)

  // These secrets should be read from a config file
  private val hosebirdAuth =
    new OAuth1(twitterConfig.apiKey, twitterConfig.apiSecret, twitterConfig.accessToken, twitterConfig.accessSecret)

  private def createHosebirdClient: BasicClient =
    new ClientBuilder()
      .name("udemy_kafka_01")
      .hosts(hosebirdHosts) // optional: mainly for the logs
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue))
      .build()

  def run(): Unit = {
    logger.info("Setup client")

    val producer = LocalKafkaProducer.newProducer()

    // Attempts to establish a connection.
    val client = createHosebirdClient
    client.connect()

    while (!client.isDone) {
      Try(msgQueue.poll(5, TimeUnit.SECONDS)) match {
        case Failure(_) =>
          client.stop()
          producer.close()
          logger.info("End of App")
        case Success(msg) if msg != null =>
          logger.info(msg)
          producer.send(
            new ProducerRecord("twitter_tweets", msg),
            (_: RecordMetadata, exception: Exception) =>
              if (exception != null) logger.error(s"Producer failed to produce a message", exception)
          )
        case _ => ()
      }
    }

  }

}
