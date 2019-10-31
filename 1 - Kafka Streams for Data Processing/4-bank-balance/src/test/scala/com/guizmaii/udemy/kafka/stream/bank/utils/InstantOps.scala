package com.guizmaii.udemy.kafka.stream.bank.utils

import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.concurrent.duration.Duration

object InstantOps {

  implicit final class InstantOps(private val instant: Instant) extends AnyVal {
    def +(duration: Duration): Instant =
      instant.plus(duration.toNanos, ChronoUnit.NANOS)
  }

}
