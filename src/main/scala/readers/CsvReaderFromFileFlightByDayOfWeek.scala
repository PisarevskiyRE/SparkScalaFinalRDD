package com.example
package readers

import schemas.FlightByDayOfWeek

object CsvReaderFromFileFlightByDayOfWeek {

  val f: Array[String] => FlightByDayOfWeek = {
    line => FlightByDayOfWeek(line(0).toInt, line(1).toDouble)
  }

  val csvReaderFromFileFlightByDayOfWeek = new CsvReaderFromFile[FlightByDayOfWeek](f)


}
