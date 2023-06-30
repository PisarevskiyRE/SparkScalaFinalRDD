package com.example
package schemas

case class DelayPercent(reason: String,
                        percent: Double) extends MetricResult with FromFile
