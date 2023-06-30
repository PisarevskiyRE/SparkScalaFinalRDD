package com.example
package writers

import schemas.{DelayPercent, MetricStore}

object CsvWriterMetricStore{
/*
* metricName: String,
top: Int,
order: String,
date: Date,
dateFrom: Date,
dateTo: Date,
path: String,
pathAll: String*/
  private val f: MetricStore => Map[String, String] = {
    line => Map(
      "1metricName" -> line.metricName,
      "2top" -> line.top.toString,
      "3order" -> line.order,
      "4date" -> line.date.toString,
      "5dateFrom" -> line.dateFrom.toString,
      "6dateTo" -> line.dateTo.toString,
      "7path" -> line.path,
      "8pathAll" -> line.pathAll
    )
  }

  val csvWriterMetricStore = new CsvWriterMetric[MetricStore](f)
}

