package com.example
package readers

import jobs.SessionWrapper
import schemas.{Airline, FromFile, MetricResult}

import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.reflect.ClassTag

class CsvReaderFromFile[A <: FromFile](f: Array[String] => A)(implicit ct: ClassTag[A])  extends  SessionWrapper {

  def read(path: String) = {

    def readFile(filename: String) =
      Source.fromFile(filename)
        .getLines()
        .drop(1)
        .filter(line => !line.isEmpty)
        .map(line => line.split(",",-1))
        .map(f)
        .toList

    val resultRDD = spark.sparkContext.parallelize[A](readFile(path))
    resultRDD
  }
}

