package com.example
package writers

import schemas.{DelayPercent, DelayReason}

object CsvWriterMetricDelayPercent{

  private val f: DelayPercent => Map[String, String] = {
    line => Map("1reason" -> line.reason, "2percent" -> line.percent.toString)
  }

  val csvWriterMetricDelayPercent = new CsvWriterMetric[DelayPercent](f)
}

