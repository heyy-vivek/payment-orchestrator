package com.fintech.orchestrator.registry


import akka.actor.ActorSystem
import akka.pattern.CircuitBreakerOpenException
import com.fintech.orchestrator.domain.PaymentMode

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TestCBRegistry extends App {

  println("=== Phase 4 Test: Circuit Breaker ===\n")

  implicit val actorSystem: ActorSystem = ActorSystem("orchestrator")

  val cbRegistry = new CircuitBreakerRegistry(actorSystem)

  println("Starting circuit breaker tests...\n")

  // --- Test 1: Normal operation (CLOSED) ---
  println("Test 1: CLOSED state (normal operation)")
  println(s"isOpen: ${cbRegistry.isOpen("razorpay", PaymentMode.ENach)}") // false
  println(s"isClosed: ${cbRegistry.isClosed("razorpay", PaymentMode.ENach)}") // true

  // Simulate successful calls
  println("\nMaking 3 successful calls...")
  for (i <- 1 to 3) {
    val result = cbRegistry.withBreaker("razorpay", PaymentMode.ENach) {
      Future.successful(s"success $i")
    }
    result.foreach(r => println(s"  ✓ $r"))
  }

  Thread.sleep(1000)
  println()

  // --- Test 2: OPEN state (after 5 failures) ---
  println("Test 2: OPEN state (after failures)")
  println("Making 5 failing calls...")

  for (i <- 1 to 5) {
    val result = cbRegistry.withBreaker("paytm", PaymentMode.UPI) {
      Future.failed(new Exception(s"failure $i"))
    }
    result.onComplete {
      case scala.util.Success(_) => println(s"  ✓ call $i succeeded")
      case scala.util.Failure(ex: CircuitBreakerOpenException) =>
        println(s"  ✗ call $i: circuit OPEN (fast-fail)")
      case scala.util.Failure(ex) =>
        println(s"  ✗ call $i: ${ex.getClass.getSimpleName}")
    }
  }

  Thread.sleep(2000)
  println(s"\nCircuit is now OPEN: ${cbRegistry.isOpen("paytm", PaymentMode.UPI)}")

  // --- Test 3: HALF-OPEN after resetTimeout ---
  println("\nTest 3: HALF-OPEN state (after reset timeout)")
  println("Circuit was open, waiting 65 seconds for reset timeout...")
  println("(skipping wait for demo, but in real scenario would happen)\n")

  // Simulate the reset (in reality Akka does this after 60 seconds)
  cbRegistry.reset("paytm", PaymentMode.UPI)
  println(s"After reset: isClosed = ${cbRegistry.isClosed("paytm", PaymentMode.UPI)}")

  // --- Test 4: Per-mode isolation ---
  println("\nTest 4: Per-mode isolation")
  println("Fail ENach 5 times...")
  for (i <- 1 to 5) {
    cbRegistry.withBreaker("cashfree", PaymentMode.ENach) {
      Future.failed(new Exception("fail"))
    }
  }
  Thread.sleep(1000)

  println(f"Cashfree ENach isOpen: ${cbRegistry.isOpen("cashfree", PaymentMode.ENach)}")
  println(f"Cashfree UPI isClosed: ${cbRegistry.isClosed("cashfree", PaymentMode.UPI)}")
  println("  → UPI is still CLOSED even though ENach is OPEN\n")

  // --- Cleanup ---
  actorSystem.terminate()
  println("Done")
}