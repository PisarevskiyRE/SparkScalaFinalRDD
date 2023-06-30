package com.example
package readers

import jobs.SessionWrapper
import schemas.{Airport, TopAirportByFlight}

import org.apache.spark.rdd.RDD

import scala.io.Source


object CsvReaderFromFileTopAirportByFlight{
  private val f: Array[String] => TopAirportByFlight = {
    values =>
      TopAirportByFlight(
        values(0),
        values(1).toInt)
  }

  val csvReaderFromFileTopAirportByFlight = new CsvReaderFromFile[TopAirportByFlight](f)
}
