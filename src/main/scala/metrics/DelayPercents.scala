package com.example
package metrics


import schemas.{DelayPercent, Flight, MetricStore}

import com.example.readers.CsvReaderFromFileDelayPercent
import org.apache.spark.rdd.RDD

import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered

class DelayPercents(flights: RDD[Flight],
                     currentMetricStore: MetricStore)  extends Metric[Flight, DelayPercent] {
  override def filterOnDate(): RDD[Flight] = {
    // смотрим по какое число уэе посчитано
    val dateTo = currentMetricStore.dateTo
    // фильтруем что нужно еще посчитать
    flights.filter(line => line.NormalizeDate > dateTo)
  }

  override def getMetric(filteredOnDate: RDD[Flight]): (RDD[DelayPercent], MetricStore) = {
    val totalDelayByReason = filteredOnDate
      .map(flight => (
        flight.AIR_SYSTEM_DELAY,
        flight.SECURITY_DELAY,
        flight.AIRLINE_DELAY,
        flight.LATE_AIRCRAFT_DELAY,
        flight.WEATHER_DELAY
      ))
      .reduce((a, b) => (
        a._1 + b._1,
        a._2 + b._2,
        a._3 + b._3,
        a._4 + b._4,
        a._5 + b._5
      ))

    val totalDelay = totalDelayByReason.productIterator.map(_.asInstanceOf[Int]).sum

    val filteredOnDateRDD = spark.sparkContext.parallelize(Seq(
      ("AIR_SYSTEM_DELAY", totalDelayByReason._1),
      ("SECURITY_DELAY", totalDelayByReason._2),
      ("AIRLINE_DELAY", totalDelayByReason._3),
      ("LATE_AIRCRAFT_DELAY", totalDelayByReason._4),
      ("WEATHER_DELAY", totalDelayByReason._5)
    ))
      .map { case (reason, delay) =>
        DelayPercent(reason, (delay * 100.0 / totalDelay).round)
      }


    implicit val ordering: Ordering[Date] = Ordering.fromLessThan[Date]((d1, d2) => d1.before(d2))

    val fromDate = filteredOnDate.map(x => x.NormalizeDate).min()
    val toDate = filteredOnDate.map(x => x.NormalizeDate).max()

    val newMetricStore = MetricStore(
      metricName = "DelayPercents",
      top = currentMetricStore.top,
      order = currentMetricStore.order,
      date = Date.valueOf(LocalDate.now().toString),
      dateFrom = Date.valueOf(fromDate.toString),
      dateTo = Date.valueOf(toDate.toString),
      path = currentMetricStore.path,
      pathAll = currentMetricStore.pathAll)

    (filteredOnDateRDD, newMetricStore)
  }

  override def mergeMetric(newMetric: RDD[DelayPercent], metricStore: MetricStore): RDD[DelayPercent] = {
    //достаем старый результат

    val projectDir = System.getProperty("user.dir")
    val relativePath = currentMetricStore.path
    val filePath = Paths.get(projectDir, relativePath).toString


    if (Files.exists(Paths.get(filePath))) {

      val oldMetric: RDD[DelayPercent] = CsvReaderFromFileDelayPercent.csvReaderFromFileDelayPercent.read(metricStore.path)

      oldMetric
        .union(newMetric)
        .map(x => (x.reason, x.percent))
        .reduceByKey(_ + _)
        .map(line => DelayPercent(line._1, line._2))

    }
    else
      newMetric

  }

  override def calculate(): (RDD[DelayPercent], RDD[DelayPercent], MetricStore) = {
    val filteredOnDate: RDD[Flight] = filterOnDate()

    val (metric, newMetricStore) = getMetric(filteredOnDate)

    val resultAll: RDD[DelayPercent] = mergeMetric(metric, currentMetricStore)


    val result: RDD[DelayPercent] = spark.sparkContext.parallelize(
      resultAll
        .collect()
        .sortBy(
          currentMetricStore.order match {
            case "desc" => -_.percent
            case "asc" => _.percent
          }
        )
        .take(currentMetricStore.top)
    )


    (resultAll, result, newMetricStore)
  }
}
object DelayPercents {
  def apply(flights: RDD[Flight],
            currentStore: MetricStore) = new DelayPercents(flights, currentStore)
}