package com.example
package readers

import jobs.SessionWrapper
import schemas.{Airport, TopAirportByFlight}

import org.apache.spark.rdd.RDD

import scala.io.Source

class CsvReaderTopAirportByFlight extends CsvReader[TopAirportByFlight] with SessionWrapper {

  override def read(path: String): RDD[TopAirportByFlight] = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(",", -1))
        .map(values => TopAirportByFlight(
          values(0),
          values(1).toInt)

        ).toList

    val TopAirportByFlightRDD: RDD[TopAirportByFlight] = spark.sparkContext.parallelize(readFile(path))

    TopAirportByFlightRDD
  }
}

object CsvReaderTopAirportByFlight{
  def apply() = new CsvReaderTopAirportByFlight()
}
