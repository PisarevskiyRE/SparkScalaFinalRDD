package com.example
package readers

import jobs.SessionWrapper
import schemas.{OnTimeAirline, TopAirportByFlight}

import org.apache.spark.rdd.RDD

import scala.io.Source


object CsvReaderFromFileOnTimeAirline{
  private val f: Array[String] => OnTimeAirline = {
    values =>
      OnTimeAirline(
        values(0),
        values(1).toInt)
  }

  val csvReaderFromFileOnTimeAirline = new CsvReaderFromFile[OnTimeAirline](f)


}
