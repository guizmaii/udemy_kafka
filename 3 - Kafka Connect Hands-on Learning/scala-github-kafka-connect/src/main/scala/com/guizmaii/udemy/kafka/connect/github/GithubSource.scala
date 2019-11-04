package com.guizmaii.udemy.kafka.connect.github

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

final class GithubSource extends SourceConnector {

  private var conf: GithubConnectorConfig = _

  override def start(props: java.util.Map[String, String]): Unit = conf = GithubConnectorConfig(props)

  override def taskClass(): Class[_ <: Task] = classOf[GithubTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] =
    util.Arrays.asList(conf.originalsStrings())

  override def stop(): Unit = ???

  override def config(): ConfigDef = GithubConnectorConfig.config

  override def version(): String = this.getClass.getPackage.getImplementationVersion
}
