package com.example
package readers

import org.apache.spark.rdd.RDD

trait CsvReader[T] {
  def read(path: String): RDD[T]
}
