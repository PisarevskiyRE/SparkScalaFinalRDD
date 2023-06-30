package com.example
package metrics

import readers.{CsvReaderFromFileDelayReason, CsvReaderFromFileOnTimeAirline}
import schemas.{DelayReason, Flight, MetricStore, OnTimeAirline}

import org.apache.spark.rdd.RDD

import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered

class DelayReasons(flights: RDD[Flight],
                     currentMetricStore: MetricStore)  extends Metric[Flight, DelayReason] {
  override def filterOnDate(): RDD[Flight] = {
    // смотрим по какое число уэе посчитано
    val dateTo = currentMetricStore.dateTo
    // фильтруем что нужно еще посчитать
    flights.filter(line => line.NormalizeDate > dateTo)
  }

  override def getMetric(filteredOnDate: RDD[Flight]): (RDD[DelayReason], MetricStore) = {
    val filteredOnDateRDD: RDD[DelayReason] = filteredOnDate

      .flatMap(flight => {
        val delays = Seq(
          ("AIR_SYSTEM_DELAY", flight.AIR_SYSTEM_DELAY),
          ("SECURITY_DELAY", flight.SECURITY_DELAY),
          ("AIRLINE_DELAY", flight.AIRLINE_DELAY),
          ("LATE_AIRCRAFT_DELAY", flight.LATE_AIRCRAFT_DELAY),
          ("WEATHER_DELAY", flight.WEATHER_DELAY)
        )
        delays.filter { case (_, delay) => delay > 0 }
      })
      .mapValues(_ => 1)
      .reduceByKey(_ + _)
      .map(line => DelayReason(line._1, line._2))


    implicit val ordering: Ordering[Date] = Ordering.fromLessThan[Date]((d1, d2) => d1.before(d2))

    val fromDate = filteredOnDate.map(x => x.NormalizeDate).min()
    val toDate = filteredOnDate.map(x => x.NormalizeDate).max()

    val newMetricStore = MetricStore(
      metricName = "DelayReasons",
      top = currentMetricStore.top,
      order = currentMetricStore.order,
      date = Date.valueOf(LocalDate.now().toString),
      dateFrom = Date.valueOf(fromDate.toString),
      dateTo = Date.valueOf(toDate.toString),
      path = currentMetricStore.path,
      pathAll = currentMetricStore.pathAll)

    (filteredOnDateRDD, newMetricStore)
  }

  override def mergeMetric(newMetric: RDD[DelayReason], metricStore: MetricStore): RDD[DelayReason] = {
    //достаем старый результат

    val projectDir = System.getProperty("user.dir")
    val relativePath = currentMetricStore.path
    val filePath = Paths.get(projectDir, relativePath).toString


    if (Files.exists(Paths.get(filePath))) {

      val oldMetric: RDD[DelayReason] = CsvReaderFromFileDelayReason.csvReaderFromFileDelayReason.read(metricStore.path)

      oldMetric
        .union(newMetric)
        .map(x => (x.reason, x.count))
        .reduceByKey(_ + _)
        .map(line => DelayReason(line._1, line._2))

    }
    else
      newMetric

  }

  override def calculate(): (RDD[DelayReason], RDD[DelayReason], MetricStore) = {
    val filteredOnDate: RDD[Flight] = filterOnDate()

    val (metric, newMetricStore) = getMetric(filteredOnDate)

    val resultAll: RDD[DelayReason] = mergeMetric(metric, currentMetricStore)


    val result: RDD[DelayReason] = spark.sparkContext.parallelize(
      resultAll
        .collect()
        .sortBy(
          currentMetricStore.order match {
            case "desc" => -_.count
            case "asc" => _.count
          }
        )
        .take(currentMetricStore.top)
    )


    (resultAll, result, newMetricStore)
  }
}
object DelayReasons {
  def apply(flights: RDD[Flight],
            currentStore: MetricStore) = new DelayReasons(flights, currentStore)
}