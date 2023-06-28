package com.example
package metrics

import schemas.{Flight, MetricStore, OnTimeAirline, TopAirportByFlight}

import com.example.readers.CsvReaderFromFileOnTimeAirline
import org.apache.spark.rdd.RDD

import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered

class OnTimeAirlines(flights: RDD[Flight],
                     currentMetricStore: MetricStore )  extends Metric[Flight, OnTimeAirline] {
  override def filterOnDate(): RDD[Flight] = {
    // смотрим по какое число уэе посчитано
    val dateTo = currentMetricStore.dateTo
    // фильтруем что нужно еще посчитать
    flights.filter(line => line.NormalizeDate > dateTo)
  }

  override def getMetric(filteredOnDate: RDD[Flight]): (RDD[OnTimeAirline], MetricStore) = {
    val filteredOnDateRDD: RDD[OnTimeAirline] = filteredOnDate
      .filter(line => line.CANCELLED == 0)
      .map(line => (line.AIRLINE, 1))
      .reduceByKey(_ + _)
      .map(line => OnTimeAirline(line._1, line._2))


    implicit val ordering: Ordering[Date] = Ordering.fromLessThan[Date]((d1, d2) => d1.before(d2))

    val fromDate = filteredOnDate.map(x => x.NormalizeDate).min()
    val toDate = filteredOnDate.map(x => x.NormalizeDate).max()

    val newMetricStore = MetricStore(
      metricName = "OnTimeAirlines",
      top = currentMetricStore.top,
      order = currentMetricStore.order,
      date = Date.valueOf(LocalDate.now().toString),
      dateFrom = Date.valueOf(fromDate.toString),
      dateTo = Date.valueOf(toDate.toString),
      path = currentMetricStore.path,
      pathAll = currentMetricStore.pathAll)

    (filteredOnDateRDD, newMetricStore)
  }

  override def mergeMetric(newMetric: RDD[OnTimeAirline], metricStore: MetricStore): RDD[OnTimeAirline] = {
    //достаем старый результат

    val projectDir = System.getProperty("user.dir")
    val relativePath = currentMetricStore.path
    val filePath = Paths.get(projectDir, relativePath).toString


    if (Files.exists(Paths.get(filePath))) {

      val oldMetric: RDD[OnTimeAirline] = CsvReaderFromFileOnTimeAirline.csvReaderFromFileOnTimeAirline.read(metricStore.path)

      oldMetric
        .union(newMetric)
        .map(x => (x.airline, x.count))
        .reduceByKey(_ + _)
        .map(line => OnTimeAirline(line._1, line._2))

    }
    else
      newMetric

  }

  override def calculate(): (RDD[OnTimeAirline], RDD[OnTimeAirline], MetricStore) = {
    val filteredOnDate: RDD[Flight] = filterOnDate()

    val (metric, newMetricStore) = getMetric(filteredOnDate)

    val resultAll: RDD[OnTimeAirline] = mergeMetric(metric, currentMetricStore)


    val result: RDD[OnTimeAirline] = spark.sparkContext.parallelize(
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
object OnTimeAirlines {
  def apply(flights: RDD[Flight],
            currentStore: MetricStore) = new OnTimeAirlines(flights, currentStore)
}