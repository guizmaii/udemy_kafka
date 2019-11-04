package com.guizmaii.udemy.kafka.connect.github

import java.util

import org.apache.kafka.connect.connector.Task

final class GithubTask extends Task {
  override def version(): String = ???

  override def start(props: util.Map[String, String]): Unit = ???

  override def stop(): Unit = ???
}
