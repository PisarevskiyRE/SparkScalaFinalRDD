package com.example
package schemas

case class DelayReason(reason: String,
                       count: Int) extends MetricResult with FromFile
