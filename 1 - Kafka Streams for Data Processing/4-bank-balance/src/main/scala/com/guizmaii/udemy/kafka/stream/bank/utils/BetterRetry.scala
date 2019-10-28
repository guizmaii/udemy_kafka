package com.guizmaii.udemy.kafka.stream.bank.utils

import cats.Monad
import retry.{RetryDetails, RetryPolicies, RetryPolicy, Sleep}

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

  def retryForeverEvery[M[_]: Monad: Sleep, A](action: => M[A], duration: FiniteDuration): M[A] =
    retryingM(action)(RetryPolicies.constantDelay(duration), _ => false, (_, _) => Monad[M].unit)

}
