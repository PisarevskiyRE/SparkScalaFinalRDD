package com.example
package readers

import schemas.Airport

object CsvReaderFromFileAirport {
  val f: Array[String] => Airport = {
    values =>
      Airport(
        values(0),
        values(1),
        values(2),
        values(3),
        values(4),
        values(5) match {
          case "" => 0
          case _ => values(5).toDouble
        },
        values(6) match {
          case "" => 0
          case _ => values(6).toDouble
        })
  }

  val csvReaderFromFileAirport = new CsvReaderFromFile[Airport](f)

}

