package com.guizmaii.udemy.kafka.connect.github

import java.util

import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

final class GithubSourceTask extends SourceTask {
  override def version(): String = ???

  override def start(props: util.Map[String, String]): Unit = ???

  override def stop(): Unit = ???

  override def poll(): util.List[SourceRecord] = ???
}
