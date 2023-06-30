package com.example
package writers

import schemas.FlightByDayOfWeek

object CsvWriterMetricFlightByDayOfWeek{

  private val f: FlightByDayOfWeek => Map[String, String] = {
    line => Map("1day" -> line.day.toString, "2delay" -> line.delay.toString)
  }

  val csvWriterMetricFlightByDayOfWeek = new CsvWriterMetric[FlightByDayOfWeek](f)
}

