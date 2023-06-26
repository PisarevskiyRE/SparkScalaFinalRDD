package com.example
package readers

import jobs.SessionWrapper
import schemas.Flight

import org.apache.spark.rdd.RDD

import java.sql.Date
import scala.io.Source


  //TODO добавить нормальный вункции для matсh и date

class CsvReaderFlights extends CsvReader[Flight] with SessionWrapper {

  override def read(path: String): RDD[Flight] = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(",",-1))
        .map(values => Flight(
          values(0).toInt,
          values(1).toInt,
          values(2).toInt,
          values(3).toInt,
          values(4),
          values(5).toInt,
          values(6),
          values(7),
          values(8),
          values(9),
          values(10),
          values(11) match {
            case "" => 0
            case _ => values(11).toInt
          },
          values(12) match {
            case "" => 0
            case _ => values(12).toInt
          },
          values(13),
          values(14) match {
            case "" => 0
            case _ => values(14).toInt
          },
          values(15) match {
            case "" => 0
            case _ => values(15).toInt
          },
          values(16) match {
            case "" => 0
            case _ => values(16).toInt
          },
          values(17) match {
            case "" => 0
            case _ => values(17).toInt
          },
          values(18),
          values(19) match {
            case "" => 0
            case _ => values(19).toInt
          },
          values(20),
          values(21),
          values(22) match {
            case "" => 0
            case _ => values(22).toInt
          },
          values(23) match {
            case "" => 0
            case _ => values(23).toInt
          },
          values(24) match {
            case "" => 0
            case _ => values(24).toInt
          },
          values(25),
          values(26),
          values(27),
          values(28),
          values(29),
          values(30),
          Date.valueOf(values(0) + "-" + values(1)+ "-" + values(2))
        )
        ).toList

    val flightRDD: RDD[Flight] = spark.sparkContext.parallelize(readFile(path))

    flightRDD
  }

}

object CsvReaderFlights{
  def apply() = new CsvReaderFlights()
}
