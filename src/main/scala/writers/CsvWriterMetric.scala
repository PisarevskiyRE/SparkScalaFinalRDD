package com.example
package writers

import jobs.SessionWrapper
import schemas.MetricResult

import org.apache.spark.rdd.RDD

import java.io.{File, PrintWriter}

class CsvWriterMetric[A <: MetricResult](f: A => Map[String, String]) extends CsvWriter[A] with SessionWrapper {

  override def write(rdd: RDD[A], outputPath: String): Unit = {

    def convertRDDToList(rdd: RDD[A]): List[Map[String, String]] = {
      rdd.collect().map(f).toList
    }

    def saveListToCsv(data: List[Map[String, String]], filePath: String): Unit = {
      val file = new File(filePath)
      val parentDir = file.getParentFile

      if (!parentDir.exists()) {
        parentDir.mkdirs()
      }

      val writer = new PrintWriter(file)
      val header = data.headOption.map(_.keys.toList.sorted).getOrElse(List.empty)
      val csvHeader = header.mkString(",")
      writer.println(csvHeader)

      data.foreach { map =>
        val csvRow = header.map(key => map.getOrElse(key, "")).mkString(",")
        writer.println(csvRow)
      }

      writer.close()
    }

    val listMap = convertRDDToList(rdd)

    saveListToCsv(listMap, outputPath)
  }
}

