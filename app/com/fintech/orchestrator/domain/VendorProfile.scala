package com.fintech.orchestrator.domain

import java.time.Instant

sealed trait HealthStatus

object HealthStatus {
  case object Healthy extends HealthStatus

  case object Degraded extends HealthStatus

  case object Down extends HealthStatus
}

sealed trait CircuitState

object CircuitState {
  case object Closed extends CircuitState // normal — calls go through

  case object Open extends CircuitState // failing — reject immediately

  case object HalfOpen extends CircuitState // recovering — let one call test
}

sealed trait RoutingStrategy

object RoutingStrategy {
  case object PriorityFirst extends RoutingStrategy

  case object CostOptimized extends RoutingStrategy

  case object SuccessRateWeighted extends RoutingStrategy

  case object RoundRobin extends RoutingStrategy
}


//vendor profile
case class VendorProfile(
                          vendorId: String,
                          name: String,
                          supportedModes: Set[PaymentMode],
                          basePriority: Int,
                          costPerCallPaise: Long,

                          healthStatus: HealthStatus = HealthStatus.Healthy,
                          circuitState: CircuitState = CircuitState.Closed,

                          successRate1h: Double = 1.0,
                          avgLatencyMs: Long = 500L,
                          totalCallsLastHour: Long = 0L,

                          failureCount: Int = 0,
                          lastFailureAt: Option[Instant] = None,
                          updatedAt: Instant = Instant.now()
                        ) {
  def supportsMode(mode: PaymentMode): Boolean =
    supportedModes.contains(mode)

  def isCallable: Boolean =
    healthStatus != HealthStatus.Down &&
      circuitState != CircuitState.Open

  def compositeScore(maxCostPaise: Long, maxLatencyMs: Long): Double = {
    val normCost = if (maxCostPaise > 0) costPerCallPaise.toDouble / maxCostPaise else 0.0
    val normLatency = if (maxLatencyMs > 0) avgLatencyMs.toDouble / maxLatencyMs else 0.0
    0.6 * successRate1h - 0.2 * normCost - 0.2 * normLatency
  }
}

object VendorProfile {
  // Seed data — used on first run before Mongo has anything
  def defaults: List[VendorProfile] = List(
    VendorProfile(
      vendorId = "razorpay",
      name = "Razorpay",
      supportedModes = Set(PaymentMode.ENach, PaymentMode.UPI),
      basePriority = 3,
      costPerCallPaise = 180
    ),
    VendorProfile(
      vendorId = "cashfree",
      name = "Cashfree",
      supportedModes = Set(PaymentMode.ENach, PaymentMode.UPI, PaymentMode.NEFT),
      basePriority = 1,
      costPerCallPaise = 150
    ),
    VendorProfile(
      vendorId = "paytm",
      name = "Paytm PG",
      supportedModes = Set(PaymentMode.ENach, PaymentMode.UPI),
      basePriority = 2,
      costPerCallPaise = 210
    )
  )
}
