package com.fintech.orchestrator.domain

import com.fintech.orchestrator.domain.PaymentMode.UPI
import com.fintech.orchestrator.domain.PaymentPriority.High


object Scratch extends App {

  // 1. Can you construct a request?
  val req = PaymentRequest(transactionId = "txn123", idempotencyKey = "idmp123", mode = UPI, amountPaise = 1450000, customerId = "cust1", accountNumber = "9088", ifsc = "SBIN10230001", description = Some("tesing the domain files"), priority = High)
  println(s"High value? ${req.isHighValue}")   // false — ₹14,500 < ₹1L
  println(s"req.description: ${req.description}")

  // 2. Can you use copy()?
  val highValueReq = req.copy(amountPaise = 10_00_000_00L)
  println(s"High value? ${highValueReq.isHighValue}")   // true

  // 3. Does compositeScore work?
  val vendor = VendorProfile.defaults.head
  val score  = vendor.compositeScore(maxCostPaise = 210, maxLatencyMs = 1000)
  println(s"Score: $score")

  // 4. Does pattern matching on sealed traits work?
  val outcome: PaymentOutcome = PaymentOutcome.Timeout
  val msg = outcome match {
    case PaymentOutcome.Success        => "all good"
    case PaymentOutcome.Pending        => "wait for webhook"
    case PaymentOutcome.Timeout        => "retry on next vendor"
    case PaymentOutcome.InvalidRequest => "stop — bad data"
    case _                             => "something else"
  }
  println(msg)   // retry on next vendor
}