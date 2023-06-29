package com.example
package metrics

import readers.{CsvReaderFromFileFlightByDayOfWeek, CsvReaderFromFileOnTimeAirline}
import schemas.{Flight, FlightByDayOfWeek, MetricStore, OnTimeAirline}

import org.apache.spark.rdd.RDD

import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered

class FlightsByDayOfWeek(flights: RDD[Flight],
                     currentMetricStore: MetricStore)  extends Metric[Flight, FlightByDayOfWeek] {
  override def filterOnDate(): RDD[Flight] = {
    // смотрим по какое число уэе посчитано
    val dateTo = currentMetricStore.dateTo
    // фильтруем что нужно еще посчитать
    flights.filter(line => line.NormalizeDate > dateTo)
  }

  override def getMetric(filteredOnDate: RDD[Flight]): (RDD[FlightByDayOfWeek], MetricStore) = {


    val filteredOnDateRDD: RDD[FlightByDayOfWeek] = filteredOnDate
      .groupBy(_.DAY_OF_WEEK) // Группируем данные по дню недели
      .mapValues(flights => flights.map(_.ARRIVAL_DELAY).sum / flights.size.toDouble)
      .map(x => FlightByDayOfWeek(x._1,x._2))


    implicit val ordering: Ordering[Date] = Ordering.fromLessThan[Date]((d1, d2) => d1.before(d2))

    val fromDate = filteredOnDate.map(x => x.NormalizeDate).min()
    val toDate = filteredOnDate.map(x => x.NormalizeDate).max()

    val newMetricStore = MetricStore(
      metricName = "FlightByDayOfWeek",
      top = currentMetricStore.top,
      order = currentMetricStore.order,
      date = Date.valueOf(LocalDate.now().toString),
      dateFrom = Date.valueOf(fromDate.toString),
      dateTo = Date.valueOf(toDate.toString),
      path = currentMetricStore.path,
      pathAll = currentMetricStore.pathAll)

    (filteredOnDateRDD, newMetricStore)
  }

  override def mergeMetric(newMetric: RDD[FlightByDayOfWeek], metricStore: MetricStore): RDD[FlightByDayOfWeek] = {
    //достаем старый результат

    val projectDir = System.getProperty("user.dir")
    val relativePath = currentMetricStore.path
    val filePath = Paths.get(projectDir, relativePath).toString


    if (Files.exists(Paths.get(filePath))) {

      val oldMetric: RDD[FlightByDayOfWeek] = CsvReaderFromFileFlightByDayOfWeek.csvReaderFromFileFlightByDayOfWeek.read(metricStore.path)

      oldMetric
        .union(newMetric)
        .map(x => (x.day, x.day))
        .reduceByKey(_ + _)
        .map(line => FlightByDayOfWeek(line._1, line._2))

    }
    else
      newMetric

  }

  override def calculate(): (RDD[FlightByDayOfWeek], RDD[FlightByDayOfWeek], MetricStore) = {
    val filteredOnDate: RDD[Flight] = filterOnDate()

    val (metric, newMetricStore) = getMetric(filteredOnDate)

    val resultAll: RDD[FlightByDayOfWeek] = mergeMetric(metric, currentMetricStore)


    val result: RDD[FlightByDayOfWeek] = spark.sparkContext.parallelize(
      resultAll
        .collect()
        .sortBy(
          currentMetricStore.order match {
            case "desc" => -_.delay
            case "asc" => _.delay
          }
        )
        .take(currentMetricStore.top)
    )


    (resultAll, result, newMetricStore)
  }
}
object FlightsByDayOfWeek {
  def apply(flights: RDD[Flight],
            currentStore: MetricStore) = new FlightsByDayOfWeek(flights, currentStore)
}