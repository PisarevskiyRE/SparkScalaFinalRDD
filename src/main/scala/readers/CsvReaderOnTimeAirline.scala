package com.example
package readers

import jobs.SessionWrapper
import schemas.{OnTimeAirline, TopAirportByFlight}

import org.apache.spark.rdd.RDD

import scala.io.Source

class CsvReaderOnTimeAirline extends CsvReader[OnTimeAirline] with SessionWrapper {

  override def read(path: String): RDD[OnTimeAirline] = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(",", -1))
        .map(values => OnTimeAirline(
          values(0),
          values(1).toInt)

        ).toList

    val OnTimeAirlineRDD: RDD[OnTimeAirline] = spark.sparkContext.parallelize(readFile(path))

    OnTimeAirlineRDD
  }
}

object CsvReaderOnTimeAirline{
  def apply() = new CsvReaderOnTimeAirline()
}
