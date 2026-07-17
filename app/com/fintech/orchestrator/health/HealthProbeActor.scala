package com.fintech.orchestrator.health

import akka.actor.{Actor, ActorLogging, Timers}
import com.fintech.orchestrator.domain.{CircuitState, HealthStatus, VendorProfile}
import com.fintech.orchestrator.registry.TempVendorRegistry.registry
import com.fintech.orchestrator.registry.VendorRegistry
import play.api.libs.ws.WSClient

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object HealthProbeActor {
  case object Tick       // message sent on timer
  case object TimerKey   // identifies the timer
}

/**
 * Runs on a 30-second schedule.
 *
 * For each vendor, fires a lightweight HEAD request to their health endpoint.
 * Uses a hysteresis pattern:
 *   - 3 consecutive failures  → mark DOWN and open circuit
 *   - 1 success after DOWN    → mark DEGRADED, half-open circuit
 *   - 3 consecutive successes → mark HEALTHY, close circuit
 *
 * This way a single blip doesn't cause immediate state changes,
 * but persistent problems are noticed quickly.
 *
 * @param registry shared VendorRegistry singleton
 * @param wsClient Play's HTTP client (injected by Guice)
 * @param ec execution context for async operations
 */
class HealthProbeActor(
                        vendorRegistry: VendorRegistry,
                        wsClient: WSClient
                      )(implicit ec: ExecutionContext)
  extends Actor with ActorLogging with Timers {

  import HealthProbeActor._

  // ── State ──────────────────────────────────────────────────────────────────
  // Track consecutive successes/failures per vendor.
  // Used to implement hysteresis — don't change state on every blip.

  private var consecutiveFailures:  Map[String, Int] = Map.empty.withDefaultValue(0)
  private var consecutiveSuccesses: Map[String, Int] = Map.empty.withDefaultValue(0)

  // Vendor health endpoint map.
  // In a real system this would come from config or the registry itself.
  // For now we hardcode it.
  private val healthEndpoints: Map[String, String] = Map(
    "razorpay" -> "https://api.razorpay.com/v1/health",
    "cashfree" -> "https://api.cashfree.com/pg/health",
    "paytm"    -> "https://pgapi.paytm.com/v3/health"
  )

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  override def preStart(): Unit = {
    timers.startTimerWithFixedDelay(TimerKey, Tick, 30.seconds)
    log.info("HealthProbeActor started — probing vendors every 30 seconds")
    // send immediate first probe
    self ! Tick
  }

  // ── Message handling ───────────────────────────────────────────────────────

  override def receive: Receive = {
    case Tick => probeAll()
  }

  // ── Probing logic ──────────────────────────────────────────────────────────

  private def probeAll(): Unit = {
    vendorRegistry.listAllVendorProfiles.foreach { vendor =>
      probe(vendor)
    }
  }

  /**
   * Ping one vendor's health endpoint.
   * Non-blocking — the response will arrive via onComplete callback.
   */
  private def probe(vendorProfile: VendorProfile): Unit = {
    val url = healthEndpoints.getOrElse(vendorProfile.vendorId, "")
    if (url.isEmpty) {
      log.debug(s"No health endpoint configured for ${vendorProfile.vendorId}")
      return
    }

    val startMs = System.currentTimeMillis()

    // Fire and forget HTTP call
    wsClient
      .url(url)
      .withRequestTimeout(5.seconds)
      .head()
      .onComplete {
        case Success(resp) if resp.status < 500 =>
          // 2xx or 3xx or 4xx — endpoint is reachable and responding
          val latency = System.currentTimeMillis() - startMs
          onProbeSuccess(vendorProfile, latency)

        case Success(resp) =>
          // 5xx — vendor is responding but in trouble
          log.warning(
            s"Health probe ${vendorProfile.vendorId} returned HTTP ${resp.status}"
          )
          onProbeFailure(vendorProfile)

        case Failure(ex) =>
          // timeout, connection refused, DNS failure, etc.
          log.warning(
            s"Health probe ${vendorProfile.vendorId} failed: ${ex.getClass.getSimpleName} ${ex.getMessage}"
          )
          onProbeFailure(vendorProfile)
      }
  }

  private def onProbeSuccess(vendor: VendorProfile, latencyMs: Long): Unit = {
    val successes = consecutiveSuccesses(vendor.vendorId) + 1
    consecutiveSuccesses = consecutiveSuccesses.updated(vendor.vendorId, successes)
    consecutiveFailures  = consecutiveFailures.updated(vendor.vendorId, 0)

    log.debug(
      s"${vendor.vendorId} probe success (${successes}/3) — ${latencyMs}ms"
    )

    // State machine — only change registry when we reach thresholds
    val (newHealth, newCircuit) = vendor.healthStatus match {

      // Vendor was DOWN, one success brings it to DEGRADED/HALF-OPEN
      // (not immediately HEALTHY — want to see sustained recovery)
      case HealthStatus.Down if successes >= 1 =>
        log.info(s"${vendor.vendorId} recovering → DEGRADED / HALF-OPEN")
        (HealthStatus.Degraded, CircuitState.HalfOpen)

      // Vendor was DEGRADED, 3 successes brings it back to HEALTHY
      case HealthStatus.Degraded if successes >= 3 =>
        log.info(s"${vendor.vendorId} fully recovered → HEALTHY / CLOSED")
        (HealthStatus.Healthy, CircuitState.Closed)

      // No state change needed
      case other =>
        (other, vendor.circuitState)
    }

    if (newHealth != vendor.healthStatus || newCircuit != vendor.circuitState) {
      registry.updateVendorHealth(
        vendorId     = vendor.vendorId,
        health       = newHealth,
        circuitState = newCircuit,
        failureCount = 0
      )
    }
  }

  private def onProbeFailure(vendor: VendorProfile): Unit = {
    val failures = consecutiveFailures(vendor.vendorId) + 1
    consecutiveFailures  = consecutiveFailures.updated(vendor.vendorId, failures)
    consecutiveSuccesses = consecutiveSuccesses.updated(vendor.vendorId, 0)

    log.debug(s"${vendor.vendorId} probe failed (${failures}/3)")

    // Only mark DOWN after 3 consecutive failures
    if (failures >= 3 && vendor.healthStatus != HealthStatus.Down) {
      log.error(s"${vendor.vendorId} failed 3 probes → DOWN / OPEN")
      registry.updateVendorHealth(
        vendorId     = vendor.vendorId,
        health       = HealthStatus.Down,
        circuitState = CircuitState.Open,
        failureCount = failures
      )
    }
  }
}