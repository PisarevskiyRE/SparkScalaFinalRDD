package com.example
package metrics

import jobs.SessionWrapper

import com.example.schemas.MetricStore
import org.apache.spark.rdd.RDD

trait Metric[A, B] extends SessionWrapper{
  /*
    1 фитровать по дате
    2 расчет метрики
    3 merge результата (загрузка прошлой+новая)
    4 сохранение результата + обновление MetricStore
   */

  def filterOnDate(): RDD[A]

  def getMetric(filteredOnDate: RDD[A]): (RDD[B], MetricStore)

  def mergeMetric(newMetric: RDD[B], metrciStore: MetricStore): RDD[B]

  def calculate(): (RDD[B], RDD[B], MetricStore)

}