package com.example
package readers




object CsvReaderFromFileTopAirlineAndAirport {

  val f: Array[String] => TopAirlineAndAirport = {
    line => TopAirlineAndAirport(  line(0), line(1), line(2).toInt)
  }

  val csvReaderFromFileTopAirlineAndAirport = new CsvReaderFromFile[TopAirlineAndAirport](f)


}
