package com.example
package readers

import jobs.Reader.ReadConfig
import jobs.SessionWrapper
import schemas.{Airline, FilePath}

import org.apache.spark.rdd.RDD

import scala.io.Source


object CsvReaderFromFileAirline {

  val f: Array[String] => Airline = {
    line => Airline(  line(0), line(1))
  }

  val csvReaderFromFileAirline = new CsvReaderFromFile[Airline](f)


}
