package com.guizmaii.udemy.kafka.stream.bank.utils

import cats.Monad
import retry._

import scala.concurrent.duration.FiniteDuration

object RetryOps {

  implicit final class RetryOps[M[_], A](private val io: M[A]) extends AnyVal {
    def retryForeverEvery(every: FiniteDuration)(implicit M: Monad[M], S: Sleep[M]): M[A] =
      retryingM[A](RetryPolicies.constantDelay(every), _ => false, (_, _) => Monad[M].unit)(io)
  }

}
