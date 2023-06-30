package com.example
package writers

import jobs.SessionWrapper
import schemas.TopAirportByFlight

import org.apache.spark.rdd.RDD

import java.io.{File, PrintWriter}
import scala.io.Source

object CsvWriterMetricTopAirportByFlight{

  private val f: TopAirportByFlight => Map[String, String] = {
    line => Map("1airport" -> line.airport, "2count" -> line.count.toString)
  }

  val csvWriterMetricOnTimeAirline = new CsvWriterMetric[TopAirportByFlight](f)
}



//class CsvWriterTopAirportByFlight extends CsvWriter[TopAirportByFlight] with SessionWrapper {
//
//  override def write(rdd: RDD[TopAirportByFlight], outputPath: String): Unit = {
//
//    def convertRDDToList(rdd: RDD[TopAirportByFlight]): List[Map[String, String]] = {
//      rdd.collect().map { airport =>
//        Map("airport" -> airport.airport, "count" -> airport.count.toString)
//      }.toList
//    }
//
//    def saveListToCsv(data: List[Map[String, String]], filePath: String): Unit = {
//      val file = new File(filePath)
//      val parentDir = file.getParentFile
//
//      if (!parentDir.exists()) {
//        parentDir.mkdirs()
//      }
//
//      val writer = new PrintWriter(file)
//      val header = data.headOption.map(_.keys.toList.sorted).getOrElse(List.empty)
//      val csvHeader = header.mkString(",")
//      writer.println(csvHeader)
//
//      data.foreach { map =>
//        val csvRow = header.map(key => map.getOrElse(key, "")).mkString(",")
//        writer.println(csvRow)
//      }
//
//      writer.close()
//    }
//
//    val listMap = convertRDDToList(rdd)
//
//    saveListToCsv(listMap, outputPath)
//
//  }
//}
//
//object CsvWriterTopAirportByFlight{
//  def apply() = new CsvWriterTopAirportByFlight()
//}
