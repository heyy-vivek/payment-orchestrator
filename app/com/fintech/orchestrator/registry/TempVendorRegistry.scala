package com.fintech.orchestrator.registry

import akka.actor.{ActorSystem, Props}

import scala.sys.runtime

object TempVendorRegistry extends App {

  println("=== VendorRegistry + VendorRegistryActor Test ===\n")

  // Step 1: Create the registry singleton
  val vendorRegistry = new VendorRegistry()
  println(s"✓ Created registry")
  println(s"  registry.size: ${vendorRegistry.size}\n")

  // Step 2: Seed with defaults
  println("Seeding defaults...")
  vendorRegistry.seedDefaults()
  println(s"✓ Seeded")
  println(s"  registry.size: ${vendorRegistry.size}\n")

  // Step 3: Look up a vendor
  println("Looking up razorpay...")
  vendorRegistry.findVendorProfile("razorpay") match {
    case Some(v) =>
      println(s"✓ Found: ${v.vendorId}")
      println(s"  health: ${v.healthStatus}")
      println(s"  circuit: ${v.circuitState}")
      println(s"  successRate: ${v.successRate1h}\n")
    case None =>
      println("✗ Not found\n")
  }

  // Step 4: Create an ActorSystem — required to run actors
  println("Starting ActorSystem...")
  implicit val system: ActorSystem = ActorSystem("orchestrator")
  println(s"✓ ActorSystem created\n")

  // Step 5: Create the actor through the system
  println("Starting VendorRegistryActor...")
  val registryActor = system.actorOf(
    Props(new VendorRegistryActor(vendorRegistry)),
    "vendor-registry-actor"
  )
  println(s"✓ VendorRegistryActor started: $registryActor\n")

  // Step 6: Let it run and log for a bit
  println("Actor will log vendor states every 60s. Waiting 5 seconds...\n")
  Thread.sleep(5000)

  // Step 7: Simulate a vendor health change
  println("\n--- Simulating health change ---")
  vendorRegistry.updateVendorHealth(
    "razorpay",
    com.fintech.orchestrator.domain.HealthStatus.Down,
    com.fintech.orchestrator.domain.CircuitState.Open,
    5
  )
  println("Updated razorpay → DOWN\n")

  // Wait to see the next refresh log it
  println(s"Waiting 60 more seconds to see updated status...\n")
  Thread.sleep(60000)

  // Step 8: Shutdown gracefully
  println("\n--- Shutting down ---")
  system.terminate()
  println("✓ ActorSystem terminated")
}


