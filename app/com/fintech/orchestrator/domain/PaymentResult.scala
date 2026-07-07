package com.fintech.orchestrator.domain

import java.time.Instant

sealed trait PaymentOutcome

object PaymentOutcome {
  case object Success extends PaymentOutcome

  case object Failure extends PaymentOutcome

  case object Pending extends PaymentOutcome // ENach is async

  case object Timeout extends PaymentOutcome

  case object VendorError extends PaymentOutcome // 5xx

  case object InvalidRequest extends PaymentOutcome // 4xx — don't retry
}

case class PaymentResult(
                          transactionId: String,
                          idempotencyKey: String,
                          outcome: PaymentOutcome,
                          vendorId: String,
                          vendorRef: Option[String] = None,
                          errorCode: Option[String] = None,
                          errorMessage: Option[String] = None,
                          latencyMs: Long = 0L,
                          fallbackUsed: Boolean = false,
                          attemptCount: Int = 1,
                          respondedAt: Instant = Instant.now()
                        ) {
  def isSuccess: Boolean = outcome == PaymentOutcome.Success

  def isPending: Boolean = outcome == PaymentOutcome.Pending

  def isRetryable: Boolean = outcome == PaymentOutcome.Timeout ||
    outcome == PaymentOutcome.VendorError

  def isNonRetryable: Boolean = outcome == PaymentOutcome.InvalidRequest
}

// One entry per vendor attempt in the fallback chain
case class VendorAttempt(
                          vendorId: String,
                          outcome: PaymentOutcome,
                          latencyMs: Long,
                          skipped: Boolean = false,
                          skipReason: Option[String] = None,
                          attemptedAt: Instant = Instant.now()
                        )