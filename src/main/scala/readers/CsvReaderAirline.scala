package com.example
package readers

import jobs.Reader.ReadConfig
import jobs.SessionWrapper
import schemas.{Airline, FilePath}

import org.apache.spark.rdd.RDD

import scala.io.Source


class CsvReaderAirline extends CsvReader[Airline] with SessionWrapper {

  override def read(path: String): RDD[Airline] = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(","))
        .map(values => Airline(
          values(0),
          values(1))
        ).toList

    val airlineRDD: RDD[Airline] = spark.sparkContext.parallelize(readFile(path))

    airlineRDD
  }
}

object CsvReaderAirline{
  def apply() = new CsvReaderAirline()
}
