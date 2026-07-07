package com.fintech.orchestrator.domain

import java.time.Instant

sealed trait PaymentMode
object PaymentMode {
  case object ENach extends PaymentMode
  case object UPI extends PaymentMode
  case object IMPS extends PaymentMode
  case object NEFT extends PaymentMode
}

sealed trait PaymentPriority
object PaymentPriority {
  case object High extends PaymentPriority
  case object Medium extends PaymentPriority
  case object Low extends PaymentPriority
}

case class PaymentRequest(
                           transactionId:  String,
                           idempotencyKey: String,
                           mode:           PaymentMode,
                           amountPaise:    Long,
                           customerId:     String,
                           accountNumber:  String,
                           ifsc:           String,
                           description:    Option[String],
                           priority:       PaymentPriority     = PaymentPriority.Medium,
                           metadata:       Map[String, String] = Map.empty,
                           createdAt:      Instant             = Instant.now()
                         ) {
  /** True when amount >= ₹1,00,000. Drives strategy selection in routing. */
  def isHighValue: Boolean = amountPaise >= 10000000L  // 1,00,000 rupees in paise

  /** Rupee amount for display only — never use this for calculations */
  def amountRupees: BigDecimal = BigDecimal(amountPaise) / 100
}