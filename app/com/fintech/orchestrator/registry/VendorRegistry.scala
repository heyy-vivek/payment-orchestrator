package com.fintech.orchestrator.registry

import com.fintech.orchestrator.domain.{CircuitState, HealthStatus, PaymentMode, VendorProfile}

import java.time.Instant
import javax.inject.Singleton
import scala.collection.concurrent.TrieMap

/**
 * VendorRegistry is the single source of truth for vendor state.
 *
 * Two layers:
 *   1. TrieMap  — in-memory, zero latency reads on every routing decision
 *   2. MongoDB  — durable storage, written to when state changes
 *
 * All routing decisions read from the TrieMap only — never from MongoDB.
 * MongoDB is only read once: on application startup to seed the TrieMap.
 */

@Singleton
class VendorRegistry {
  // TrieMap is like a HashMap but safe to read/write from multiple
  // threads simultaneously — important because Akka actors and HTTP
  // request threads will all be touching this at the same time.
  //
  // Key   = vendorId (e.g. "razorpay")
  // Value = the full VendorProfile

  private val vendorRegistryCache: TrieMap[String,VendorProfile] = TrieMap.empty

  //get all the available vendors
  def listAllVendorProfiles: List[VendorProfile] = vendorRegistryCache.values.toList

  //find the vendor profile with the given vendorId
  def findVendorProfile(vendorId: String): Option[VendorProfile] = vendorRegistryCache.get(vendorId)

  /**
   * The method that RoutingEngine calls.
   * Returns only vendors that:
   *   - support the requested payment mode
   *   - are not marked DOWN
   *   - do not have an OPEN circuit breaker
   * Sorted by basePriority descending so highest priority comes first.
   */
  def listEligibleVendorProfiles(paymentMode: PaymentMode): List[VendorProfile] =
    vendorRegistryCache
      .values
      .filter(_.supportsMode(mode = paymentMode))
      .filter(_.isCallable)
      .toList
      .sortBy(-_.basePriority)


  /** Add or fully replace a vendor profile */
  def addVendorProfile(profile: VendorProfile): Unit =
    vendorRegistryCache.update(profile.vendorId, profile.copy(updatedAt = Instant.now()))

  /** Remove a vendor from the registry */
  def removeVendorProfile(vendorId: String): Unit =
    vendorRegistryCache.remove(vendorId)

  /**
   * Partial update — only health and circuit fields.
   * Called by HealthProbeActor every 30 seconds.
   * Returns true if the vendor was found and updated, false if not found.
   */
  def updateVendorHealth(
                    vendorId:     String,
                    health:       HealthStatus,
                    circuitState: CircuitState,
                    failureCount: Int
                  ): Boolean =
    vendorRegistryCache.get(vendorId) match {
      case Some(existing) =>
        vendorRegistryCache.update(
          vendorId,
          existing.copy(
            healthStatus  = health,
            circuitState  = circuitState,
            failureCount  = failureCount,
            updatedAt = Instant.now()
          )
        )
        true
      case None =>
        false
    }

  /**
   * Partial update — only the rolling stats fields.
   * Called by VendorStatsActor every 60 seconds after recalculating
   * the 1-hour rolling window from real traffic.
   */
  def updateVendorStats(
                   vendorId:      String,
                   successRate1h: Double,
                   avgLatencyMs:  Long,
                   totalCalls1h:  Long
                 ): Boolean =
    vendorRegistryCache.get(vendorId) match {
      case Some(existing) =>
        vendorRegistryCache.update(
          vendorId,
          existing.copy(
            successRate1h      = successRate1h,
            avgLatencyMs       = avgLatencyMs,
            totalCallsLastHour = totalCalls1h
          )
        )
        true
      case None =>
        false
    }

  // ── Bootstrap ──────────────────────────────────────────────────────────────

  /**
   * Seed the registry with default vendor profiles.
   * Called on first startup before MongoDB has any data.
   */
  def seedDefaults(): Unit =
    VendorProfile.defaults.foreach(v => vendorRegistryCache.update(v.vendorId, v))

  /** How many vendors are currently in the registry */
  def size: Int = vendorRegistryCache.size

  /** Wipe everything — used in tests only */
  def clear(): Unit = vendorRegistryCache.clear()
}