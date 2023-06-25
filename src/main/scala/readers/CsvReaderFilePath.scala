package com.example
package readers

import schemas.FilePath

import com.example.jobs.Reader.ReadConfig
import com.example.jobs.SessionWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

// todo использовать конфиг для чтения
class CsvReaderFilePath(readConfig: ReadConfig) extends CsvReader[FilePath] with SessionWrapper {

  override def read(path: String): RDD[FilePath] = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(","))
        .map(values => FilePath(
          values(0),
          values(1))
        ).toList

    val filePathRDD: RDD[FilePath] = spark.sparkContext.parallelize(readFile(path))

    filePathRDD
  }

  def getPathByName(RDD: RDD[FilePath], name: String): String = {
    RDD
      .filter(line => line.name == name)
      .map(x => x.path)
      .first()
  }
}
