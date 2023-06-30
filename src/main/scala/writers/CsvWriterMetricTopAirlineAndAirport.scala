package com.example
package writers

import schemas.OnTimeAirline

object CsvWriterMetricTopAirlineAndAirport{

  private val f: TopAirlineAndAirport => Map[String, String] = {
    line => Map("1airline" -> line.airline,"2airport" -> line.airport, "3count" -> line.count.toString)
  }

  val csvWriterMetricTopArlineAndAirport = new CsvWriterMetric[TopAirlineAndAirport](f)
}

