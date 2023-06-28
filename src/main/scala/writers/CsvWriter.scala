package com.example
package writers

import org.apache.spark.rdd.RDD


trait CsvWriter[A] {
  def write(rdd: RDD[A], outputPath: String): Unit
}