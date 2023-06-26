package com.example
package readers

import jobs.SessionWrapper
import schemas.MetricStore

import org.apache.spark.rdd.RDD

import java.sql.Date
import scala.io.Source


class CsvReaderMetricStore extends CsvReader[MetricStore] with SessionWrapper {

  override def read(path: String): RDD[MetricStore] = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(","))
        .map(values => MetricStore(
          values(0),
          values(1).toInt,
          values(2),
          Date.valueOf(values(3)),
          Date.valueOf(values(4)),
          Date.valueOf(values(5)),
          values(6),
          values(7))
        ).toList

    val MetricStoreRDD: RDD[MetricStore] = spark.sparkContext.parallelize(readFile(path))

    MetricStoreRDD
  }
}

object CsvReaderMetricStore{
  def apply() = new CsvReaderMetricStore()

  def getMetricStoreByName(rdd: RDD[MetricStore], name: String): MetricStore = {
    rdd
      .filter(line => line.metricName == name)
      .first()
  }
}
