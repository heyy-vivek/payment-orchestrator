package com.fintech.orchestrator.health

import akka.actor.{Actor, ActorLogging, Timers}
import com.fintech.orchestrator.domain.CircuitState.HalfOpen
import com.fintech.orchestrator.domain._
import com.fintech.orchestrator.registry.VendorRegistry

import scala.concurrent.duration._
import java.time.Instant

object VendorStatsActor {

  // Message: record one vendor call result
  case class RecordCall(
                         vendorId:  String,
                         outcome:   PaymentOutcome,
                         latencyMs: Long,
                         timestamp: Instant = Instant.now()
                       )

  // Message: flush/recalculate the window
  case object FlushWindow
  case object FlushTimerKey

  // Internal: one call record in the rolling window
  private case class CallRecord(
                                 success:   Boolean,
                                 latencyMs: Long,
                                 timestamp: Instant
                               )
}

/**
 * Maintains a 1-hour sliding window of call results per vendor.
 *
 * Every transaction outcome is recorded via RecordCall message.
 * Every 60 seconds:
 *   - prune records older than 1 hour
 *   - recalculate successRate1h and avgLatencyMs
 *   - update registry
 *   - auto-degrade vendors if success rate drops <70%
 *
 * This is MORE ACCURATE than active probes because it reflects
 * real production traffic, not synthetic health checks.
 *
 * @param vendorRegistry shared VendorRegistry singleton
 */
class VendorStatsActor(vendorRegistry: VendorRegistry)
  extends Actor with ActorLogging with Timers {

  import VendorStatsActor._

  // ── State ──────────────────────────────────────────────────────────────────
  // vendorId → vector of (success, latency, timestamp) tuples
  // Ordered by timestamp (oldest first, newest last) for efficient pruning
  private var windows: Map[String, Vector[CallRecord]] =
    Map.empty.withDefaultValue(Vector.empty)

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  override def preStart(): Unit = {
    timers.startTimerWithFixedDelay(FlushTimerKey, FlushWindow, 60.seconds)
    log.info("VendorStatsActor started — 1-hour rolling window active")
  }

  // ── Message handling ───────────────────────────────────────────────────────

  override def receive: Receive = {

    case RecordCall(vendorId, outcome, latencyMs, timestamp) =>
      val isSuccess = outcome match {
        case PaymentOutcome.Success | PaymentOutcome.Pending => true
        case _                                               => false
      }
      val record  = CallRecord(isSuccess, latencyMs, timestamp)
      val current = windows(vendorId)
      windows = windows.updated(vendorId, current :+ record)

      // Debug log — will be noisy in production, use trace level
      log.debug(
        s"Recorded call for $vendorId: " +
          s"outcome=${outcome.toString.take(10)} latency=${latencyMs}ms " +
          s"(${current.size + 1} calls in window)"
      )

    case FlushWindow =>
      flush()
  }

  // ── Flushing logic ─────────────────────────────────────────────────────────

  private def flush(): Unit = {
    val cutoff = Instant.now().minusSeconds(60*60)  // 1 hour => 60*60 secs

    windows = windows.map { case (vendorId, records) =>
      // Keep only records from the last hour
      val pruned = records.filter(_.timestamp.isAfter(cutoff))

      if (pruned.nonEmpty) {
        // Recalculate stats from the pruned window
        val successCount = pruned.count(_.success)
        val successRate  = successCount.toDouble / pruned.size
        val avgLatency   = pruned.map(_.latencyMs).sum / pruned.size

        log.info(
          s"[stats] $vendorId | " +
            s"calls=${pruned.size} | " +
            s"success=${f"${successRate * 100}%.1f%%"} | " +
            s"avgLatency=${avgLatency}ms"
        )

        // Update the registry
        vendorRegistry.updateVendorStats(
          vendorId      = vendorId,
          successRate1h = BigDecimal(successRate).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble,
          avgLatencyMs  = avgLatency,
          totalCalls1h  = pruned.size.toLong
        )

        // Auto-degrade if success rate drops
        vendorRegistry.findVendorProfile(vendorId).foreach { vendor =>
          if (successRate < 0.70 && vendor.healthStatus == HealthStatus.Healthy) {
            log.warning(
              s"$vendorId success rate dropped to ${f"${successRate * 100}%.1f%%"} " +
                s"(<70% threshold) → auto-DEGRADED"
            )
            vendorRegistry.updateVendorHealth(
              vendorId     = vendorId,
              health       = HealthStatus.Degraded,
              circuitState = CircuitState.HalfOpen,
              failureCount = vendor.failureCount
            )
          } else if (successRate >= 0.95 && vendor.healthStatus == HealthStatus.Degraded) {
            log.info(
              s"$vendorId success rate recovered to ${f"${successRate * 100}%.1f%%"} " +
                s"(>95% threshold) → HEALTHY"
            )
            vendorRegistry.updateVendorHealth(
              vendorId     = vendorId,
              health       = HealthStatus.Healthy,
              circuitState = CircuitState.Closed,
              failureCount = 0
            )
          }
        }
      } else {
        // No data in the last hour
        log.debug(s"$vendorId has no calls in the last hour")
      }

      vendorId -> pruned
    }
  }
}