package com.guizmaii.udemy.kafka.stream.bank.utils

import cats.Monad
import cats.effect.IO
import retry.{ RetryDetails, RetryPolicies, RetryPolicy, Sleep }

import scala.concurrent.duration.FiniteDuration

object BetterRetry {

  /**
   * In this form, the Scalac type inference works better
   */
  def retryingM[M[_]: Monad: Sleep, A](
    action: => M[A]
  )(
    policy: RetryPolicy[M],
    wasSuccessful: A => Boolean,
    onFailure: (A, RetryDetails) => M[Unit]
  ): M[A] = retry.retryingM(policy, wasSuccessful, onFailure)(action)

  implicit final class RetryOps[M[_], A](private val io: M[A]) extends AnyVal {
    def retryForeverEvery(every: FiniteDuration)(implicit M: Monad[M], S: Sleep[M]): M[A] =
      retryingM(io)(RetryPolicies.constantDelay(every), _ => false, (_, _) => Monad[M].unit)
  }

}
