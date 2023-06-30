package com.example
package readers

import schemas.DelayReason


object CsvReaderFromFileDelayReason{
  private val f: Array[String] => DelayReason = {
    values =>
      DelayReason(
        values(0),
        values(1).toInt)
  }

  val csvReaderFromFileDelayReason = new CsvReaderFromFile[DelayReason](f)


}
