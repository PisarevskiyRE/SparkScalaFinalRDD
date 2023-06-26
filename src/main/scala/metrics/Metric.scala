package com.example
package metrics

import jobs.SessionWrapper

import com.example.schemas.MetricStore
import org.apache.spark.rdd.RDD

trait Metric[T] extends SessionWrapper{
  /*
    1 фитровать по дате
    2 расчет метрики
    3 merge результата (загрузка прошлой+новая)
    4 сохранение результата + обновление MetricStore
   */


  // фильтруем DF по дате
  def filterOnDate(): RDD[T]

  def getMetric(filteredOnDate: RDD[T]): RDD[T]

  def mergeMetric(newMetric: RDD[T], metrciStore: MetricStore): Option[RDD[T]]

  def requiredMetric(metric: RDD[T]): RDD[T]

  def calculate(): (RDD[T], RDD[T], MetricStore)

}