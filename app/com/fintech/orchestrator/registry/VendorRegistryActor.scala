package com.fintech.orchestrator.registry

import akka.actor.{Actor, ActorLogging, Timers}
import com.fintech.orchestrator.registry.VendorRegistryActor.{Refresh, TimerKey}

import scala.concurrent.duration.DurationInt

/**
 * Runs on a 60-second timer.
 * On each tick it checks the registry and logs current vendor states.
 *
 * In Phase 3 this will also trigger health probes.
 * In Phase 4 this will sync with MongoDB.
 * For now it gives you visibility into what the registry contains.
 */

object VendorRegistryActor {
  // Messages this actor understands
  case object Refresh // sent on timer tick

  case object TimerKey // identifies the timer (so we can cancel it)
}

class VendorRegistryActor(vendorRegistry: VendorRegistry) extends Actor with ActorLogging with Timers {


  override def preStart(): Unit = {
    // Called once when the actor starts — set up the recurring timer
    timers.startTimerWithFixedDelay(key = TimerKey, msg = Refresh, delay = 60.seconds)
    log.info("VendorRegistryActor started — refreshing every 60s")

    // also do one immediate refresh on startup
    self ! Refresh
  }

  override def receive: Receive = {
    case Refresh => refresh()
  }

  private def refresh(): Unit = {
    val allVendorList = vendorRegistry.listAllVendorProfiles
    if (allVendorList.isEmpty) {
      log.warning(s"vendorList is empty - seeding vendor registry")
      vendorRegistry.seedDefaults()
      log.info(s"seeded registry with ${vendorRegistry.size} vendors")
    }
    else {
      allVendorList.foreach {
        vendor =>
          log.info(
            s"[registry] ${vendor.vendorId} | " +
              s"health=${vendor.healthStatus} | " +
              s"circuit=${vendor.circuitState} | " +
              s"successRate=${vendor.successRate1h} | " +
              s"latency=${vendor.avgLatencyMs}ms"
          )
      }
    }
  }
}