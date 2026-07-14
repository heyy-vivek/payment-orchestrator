package com.fintech.orchestrator.registry

import com.fintech.orchestrator.domain._
import com.fintech.orchestrator.registry._

object TempVendorRegistry extends App {

  val registry = new VendorRegistry()
  println(s"registry.size: ${registry.size}")

  println(s"Seeding the registry...")
  registry.seedDefaults()
  println(s"registry.size: ${registry.size}")

  println("get razorpay")
  println(s"razorpay: ${registry.findVendorProfile("razorpay")}")

}


