package com.example
package readers

import jobs.SessionWrapper
import schemas.MetricStore

import org.apache.spark.rdd.RDD

import java.sql.Date
import scala.io.Source



object CsvReaderFromFileMetricStore{

  private val f: Array[String] => MetricStore = {
    values =>
      MetricStore(
        values(0),
        values(1).toInt,
        values(2),
        Date.valueOf(values(3)),
        Date.valueOf(values(4)),
        Date.valueOf(values(5)),
        values(6),
        values(7))
  }

  val csvReaderFromFileMetricStore = new CsvReaderFromFile[MetricStore](f)


  def getMetricStoreByName(rdd: RDD[MetricStore], name: String): MetricStore = {
    rdd
      .filter(line => line.metricName == name)
      .first()
  }
}
