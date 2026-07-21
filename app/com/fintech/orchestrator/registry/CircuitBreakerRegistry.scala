package com.fintech.orchestrator.registry

import akka.actor.{ActorSystem, Scheduler}
import akka.pattern.CircuitBreaker
import com.fintech.orchestrator.domain.PaymentMode
import org.slf4j.LoggerFactory

import javax.inject.{Inject, Singleton}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Manages one Akka CircuitBreaker per (vendorId, PaymentMode) pair.
 *
 * Why per-mode?
 *   A vendor's ENach gateway being flaky should not trip the circuit
 *   for their UPI or IMPS endpoints — they're independent services.
 *
 * Thresholds (all configurable, hardcoded here for simplicity):
 *   maxFailures  = 5   — open after 5 consecutive failures
 *   callTimeout  = 10s — timeout waiting for vendor response
 *   resetTimeout = 60s — time in OPEN before testing HALF-OPEN
 *
 * Usage in the orchestrator:
 *   // Check if circuit is open before even trying to call
 *   if (!cbRegistry.isOpen("razorpay", PaymentMode.ENach)) {
 *     // Wrap the vendor call
 *     cbRegistry.withBreaker("razorpay", PaymentMode.ENach) {
 *       vendorClient.call(request)
 *     }
 *   }
 */

@Singleton
class CircuitBreakerRegistry @Inject()(
                                        actorSystem: ActorSystem
                                      )(implicit ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  // ── Configuration ──────────────────────────────────────────────────────────
  // In production, these would come from application.conf
  private val maxFailures:  Int      = 5
  private val callTimeout:  FiniteDuration = 20.seconds
  private val resetTimeout: FiniteDuration = 60.seconds

  // ── State ──────────────────────────────────────────────────────────────────
  // Cache of live circuit breakers.
  // Key   = (vendorId, PaymentMode)
  // Value = the CircuitBreaker instance
  //
  // TrieMap is thread-safe — multiple request threads will be checking
  // and updating this simultaneously.
  private val circuitBreaker: TrieMap[(String, PaymentMode), CircuitBreaker] = TrieMap.empty

  // ── Public API ─────────────────────────────────────────────────────────────

  /**
   * Wrap a vendor call with the correct circuit breaker.
   * Creates the CB lazily on first use.
   *
   * Usage:
   *   cbRegistry.withBreaker("razorpay", PaymentMode.ENach) {
   *     vendorClient.call(request)
   *   }
   *
   * If circuit is OPEN, throws akka.pattern.CircuitBreakerOpenException
   * If call times out, throws java.util.concurrent.TimeoutException
   * Otherwise returns the Future result of the call
   */
  def withBreaker[T](vendorId: String, mode: PaymentMode)(
    call: => Future[T]
  ): Future[T] = {
    val cb = getOrCreateCircuitBreaker(vendorId, mode)
    cb.withCircuitBreaker(call)
  }

  /**
   * True if this circuit is OPEN — circuit breaker will reject calls immediately
   * without even trying to contact the vendor.
   *
   * Check this before calling withBreaker to avoid the exception.
   */
  def isOpen(vendorId: String, mode: PaymentMode): Boolean =
    circuitBreaker.get((vendorId, mode)).exists(_.isOpen)

  def isHalfOpen(vendorId: String, mode: PaymentMode): Boolean =
    circuitBreaker.get((vendorId, mode)).exists(_.isHalfOpen)

  def isClosed(vendorId: String, mode: PaymentMode): Boolean =
    circuitBreaker.get((vendorId, mode)).forall(_.isClosed)

  /**
   * Manual reset — useful from admin API or incident management.
   * Force-closes the circuit and resets failure counter.
   * Next call will test the vendor normally.
   */
  def reset(vendorId: String, mode: PaymentMode): Unit = {
    val key = (vendorId, mode)
    circuitBreaker.get(key).foreach { _ =>
      // Remove and recreate — effectively resets it
      circuitBreaker.remove(key)

      val newCB = buildCircuitBreaker(vendorId = vendorId, mode = mode)
      circuitBreaker.update((vendorId,mode),newCB)

      log.info(s"Circuit breaker reset for ($vendorId, $mode)")
    }
  }

  // ── Private ────────────────────────────────────────────────────────────────

  /**
   * Get existing breaker or create a new one.
   * Lazy creation — only creates when first needed.
   */
  private def getOrCreateCircuitBreaker(
                                  vendorId: String,
                                  mode: PaymentMode
                                ): CircuitBreaker = {
    val key = (vendorId, mode)
    circuitBreaker.getOrElseUpdate(key, buildCircuitBreaker(vendorId, mode))
  }

  /**
   * Build a new CircuitBreaker with callbacks for state changes.
   */
  private def buildCircuitBreaker(vendorId: String, mode: PaymentMode): CircuitBreaker = {
    val cb = CircuitBreaker(
      scheduler    = actorSystem.scheduler,
      maxFailures  = maxFailures,
      callTimeout  = callTimeout,
      resetTimeout = resetTimeout
    )

    // Log state transitions
    cb.onOpen {
      log.warn(
        s"Circuit OPENED for ($vendorId, $mode) — " +
          s"failing fast, not calling vendor for next 60s"
      )
    }

    cb.onHalfOpen {
      log.info(
        s"Circuit HALF-OPEN for ($vendorId, $mode) — " +
          s"sending one probe call to test recovery"
      )
    }

    cb.onClose {
      log.info(s"Circuit CLOSED for ($vendorId, $mode) — vendor recovered")
    }

    cb
  }
}