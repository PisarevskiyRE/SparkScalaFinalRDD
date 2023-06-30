package com.example
package readers

import schemas.FilePath

import com.example.jobs.Reader.ReadConfig
import com.example.jobs.SessionWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

// todo использовать конфиг для чтения
object CsvReaderFromFileFilePath {

  private val f: Array[String] => FilePath = {
    line => FilePath(line(0), line(1))
  }

  val csvReaderFromFileFilePath = new CsvReaderFromFile[FilePath](f)


  def getPathByName(RDD: RDD[FilePath], name: String): String = {
    RDD
      .filter(line => line.name == name)
      .map(x => x.path)
      .first()
  }
}
