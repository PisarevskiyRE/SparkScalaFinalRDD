package com.example
package writers

import schemas.OnTimeAirline

object CsvWriterMetricTopAirlineAndAirport{

  val f: TopAirlineAndAirport => Map[String, String] = {
    line => Map("airline" -> line.airline,"airport" -> line.airport, "count" -> line.count.toString)
  }

  val csvWriterMetricTopArlineAndAirport = new CsvWriterMetric[TopAirlineAndAirport](f)
}

