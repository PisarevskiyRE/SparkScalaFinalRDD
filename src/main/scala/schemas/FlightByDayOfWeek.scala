package com.example
package schemas


case class FlightByDayOfWeek(day: Int, delay: Double) extends MetricResult with FromFile
