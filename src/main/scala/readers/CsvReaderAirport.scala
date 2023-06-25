package com.example
package readers

import jobs.SessionWrapper
import schemas.Airport

import org.apache.spark.rdd.RDD

import scala.io.Source

// IATA_CODE,
// AIRPORT,
// CITY,
// STATE,
// COUNTRY,
// LATITUDE,
// LONGITUDE
class CsvReaderAirport extends CsvReader[Airport] with SessionWrapper {

  override def read(path: String): RDD[Airport] = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(",", -1))
        .map(values => Airport(
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
        ).toList

    val airportRDD: RDD[Airport] = spark.sparkContext.parallelize(readFile(path))

    airportRDD
  }

}

object CsvReaderAirport{
  def apply() = new CsvReaderAirport()
}
