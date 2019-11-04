package com.guizmaii.udemy.kafka.connect.github

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

class GithubSource extends SourceConnector {
  override def start(props: java.util.Map[String, String]): Unit = ???

  override def taskClass(): Class[_ <: Task] = ???

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = ???

  override def stop(): Unit = ???

  override def config(): ConfigDef = ???

  override def version(): String = ???
}
