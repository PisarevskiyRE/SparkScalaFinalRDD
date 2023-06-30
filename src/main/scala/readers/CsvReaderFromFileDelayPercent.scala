package com.example
package readers

import schemas.{DelayPercent, DelayReason}


object CsvReaderFromFileDelayPercent{
  private val f: Array[String] => DelayPercent = {
    values =>
      DelayPercent(
        values(0),
        values(1).toDouble)
  }

  val csvReaderFromFileDelayPercent = new CsvReaderFromFile[DelayPercent](f)


}
