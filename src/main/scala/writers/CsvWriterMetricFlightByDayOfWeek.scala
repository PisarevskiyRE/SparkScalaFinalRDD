package com.example
package writers

import schemas.FlightByDayOfWeek

object CsvWriterMetricFlightByDayOfWeek{

  val f: FlightByDayOfWeek => Map[String, String] = {
    line => Map("day" -> line.day.toString, "delay" -> line.delay.toString)
  }

  val csvWriterMetricFlightByDayOfWeek = new CsvWriterMetric[FlightByDayOfWeek](f)
}

