package com.fintech.orchestrator.domain

import java.time.Instant

case class RoutingEvent(
                         eventId: String,
                         transactionId: String,
                         idempotencyKey: String,
                         paymentMode: PaymentMode,
                         amountPaise: Long,
                         strategyUsed: RoutingStrategy,
                         vendorSelected: String,
                         vendorsSkipped: List[SkippedVendor],
                         attempts: List[VendorAttempt],
                         finalOutcome: PaymentOutcome,
                         fallbackUsed: Boolean,
                         totalLatencyMs: Long,
                         costPaise: Long,
                         occurredAt: Instant = Instant.now()
                       )

case class SkippedVendor(
                          vendorId: String,
                          reason: SkipReason
                        )

sealed trait SkipReason

object SkipReason {
  case object CircuitOpen extends SkipReason

  case object HealthDown extends SkipReason

  case object ModeNotSupported extends SkipReason

  case object LowSuccessRate extends SkipReason
}