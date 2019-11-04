package com.guizmaii.udemy.kafka.connect.github

import zio.{App, ZEnv, ZIO}

object Main extends App {


  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    val program = ZIO.unit

    program.fold(_ => 1, _ => 0)
  }
}
