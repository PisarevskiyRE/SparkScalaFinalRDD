package com.example
package writers

import jobs.SessionWrapper
import schemas.{OnTimeAirline, TopAirportByFlight}

import org.apache.spark.rdd.RDD

import java.io.{File, PrintWriter}

object CsvWriterMetricOnTimeAirline{

  val f: OnTimeAirline => Map[String, String] = {
    line => Map("airport" -> line.airline, "count" -> line.count.toString)
  }

  val csvWriterMetricOnTimeAirline = new CsvWriterMetric[OnTimeAirline](f)
}

