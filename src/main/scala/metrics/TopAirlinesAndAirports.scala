package com.example
package metrics

import com.example.readers.CsvReaderFromFileTopAirlineAndAirport
import com.example.schemas.{Flight, MetricStore}
import org.apache.spark.rdd.RDD

import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered

class TopAirlinesAndAirports(flights: RDD[Flight],
                             currentMetricStore: MetricStore)  extends Metric[Flight, TopAirlineAndAirport] {
  override def filterOnDate(): RDD[Flight] = {
    // смотрим по какое число уэе посчитано
    val dateTo = currentMetricStore.dateTo
    // фильтруем что нужно еще посчитать
    flights.filter(line => line.NormalizeDate > dateTo)
  }

  override def getMetric(filteredOnDate: RDD[Flight]): (RDD[TopAirlineAndAirport], MetricStore) = {

    implicit val ordering: Ordering[Date] = Ordering.fromLessThan[Date]((d1, d2) => d1.before(d2))

    val filteredOnDateRDD: RDD[TopAirlineAndAirport] = filteredOnDate
      .filter(line => line.DEPARTURE_DELAY  <= 0)
      .flatMap(flight => Seq(
        ((flight.AIRLINE, flight.ORIGIN_AIRPORT), 1),
        ((flight.AIRLINE, flight.DESTINATION_AIRPORT), 1)
      ))
      .reduceByKey(_ + _)
      .map(line => TopAirlineAndAirport(line._1._1, line._1._2, line._2))




    val fromDate = filteredOnDate.map(x => x.NormalizeDate).min()
    val toDate = filteredOnDate.map(x => x.NormalizeDate).max()

    val newMetricStore = MetricStore(
      metricName = "TopArlinesAndAirports",
      top = currentMetricStore.top,
      order = currentMetricStore.order,
      date = Date.valueOf(LocalDate.now().toString),
      dateFrom = Date.valueOf(fromDate.toString),
      dateTo = Date.valueOf(toDate.toString),
      path = currentMetricStore.path,
      pathAll = currentMetricStore.pathAll)

    (filteredOnDateRDD, newMetricStore)
  }

  override def mergeMetric(newMetric: RDD[TopAirlineAndAirport], metricStore: MetricStore): RDD[TopAirlineAndAirport] = {
    //достаем старый результат

    val projectDir = System.getProperty("user.dir")
    val relativePath = currentMetricStore.path
    val filePath = Paths.get(projectDir, relativePath).toString


    if (Files.exists(Paths.get(filePath))) {

      val oldMetric: RDD[TopAirlineAndAirport] = CsvReaderFromFileTopAirlineAndAirport.csvReaderFromFileTopAirlineAndAirport.read(metricStore.path)

      oldMetric
        .union(newMetric)
        .map(x => ((x.airline,x.airport), x.count))
        .reduceByKey(_ + _)
        .map(line =>  TopAirlineAndAirport(line._1._1, line._1._2, line._2))

    }
    else
      newMetric

  }

  override def calculate(): (RDD[TopAirlineAndAirport], RDD[TopAirlineAndAirport], MetricStore) = {
    val filteredOnDate: RDD[Flight] = filterOnDate()

    val (metric, newMetricStore) = getMetric(filteredOnDate)

    val resultAll: RDD[TopAirlineAndAirport] = mergeMetric(metric, currentMetricStore)


    val result: RDD[TopAirlineAndAirport] = spark.sparkContext.parallelize(
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
object TopAirlinesAndAirports {
  def apply(flights: RDD[Flight],
            currentStore: MetricStore) = new TopAirlinesAndAirports(flights, currentStore)
}