package com.example
package writers

import schemas.DelayReason

object CsvWriterMetricDelayReasons{

  private val f: DelayReason => Map[String, String] = {
    line => Map("1reason" -> line.reason, "2count" -> line.count.toString)
  }

  val csvWriterMetricDelayReasons = new CsvWriterMetric[DelayReason](f)
}

