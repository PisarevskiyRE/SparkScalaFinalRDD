package com.example
package metrics
import schemas.{Airport, Flight, MetricStore, TopAirportByFlight}

import com.example.readers.CsvReaderTopAirportByFlight
import org.apache.arrow.flatbuf.Timestamp
import org.apache.spark.rdd.RDD

import java.nio.file.{Files, Paths}
import java.sql.Date
import java.time.{LocalDate, LocalDateTime}
import scala.math.Ordered.orderingToOrdered

class TopAirportsByFlights(flights: RDD[Flight],
                           currentMetricStore: MetricStore ) {

  private def filterOnDate(): RDD[Flight] = {
    // смотрим по какое число уэе посчитано
    val dateTo = currentMetricStore.dateTo
    // фильтруем что нужно еще посчитать
    flights.filter(line => line.NormalizeDate > dateTo)
  }

  def getMetric(filteredOnDate: RDD[Flight]): (RDD[TopAirportByFlight], MetricStore) = {

    val filteredOnDateRDD = filteredOnDate
      .map(line => (line.ORIGIN_AIRPORT, 1))
      .reduceByKey(_ + _)
      .map(line => TopAirportByFlight(line._1, line._2))

    val fromDate = filteredOnDate.map(x=>x.NormalizeDate).min()
    val toDate = filteredOnDate.map(x=>x.NormalizeDate).max()

    val newMetrciStore = MetricStore(
      metricName = "TopAirportsByFlights",
      top = 10,
      order = currentMetricStore.order,
      date = Date.valueOf(LocalDate.now().toString),
      dateFrom = Date.valueOf(fromDate.toString),
      dateTo = Date.valueOf(toDate.toString),
      path = currentMetricStore.path,
      pathAll = currentMetricStore.pathAll)

    (filteredOnDateRDD, newMetrciStore)

  }

  def mergeMetric(newMetric: RDD[TopAirportByFlight], metricStore: MetricStore): RDD[TopAirportByFlight] = {

    //достаем старый результат

    val projectDir = System.getProperty("user.dir")
    val relativePath = currentMetricStore.path
    val filePath = Paths.get(projectDir, relativePath).toString


    if (Files.exists(Paths.get(filePath))) {

      val oldMetric: RDD[TopAirportByFlight] = CsvReaderTopAirportByFlight().read(metricStore.path)


      oldMetric
        .union(newMetric)
        .map(x => (x.airport, x.count))
        .reduceByKey(_ + _)
        .map(line => TopAirportByFlight(line._1, line._2))


    }
    else
      newMetric

  }

  def requiredMetric(metric: RDD[Flight]): RDD[Flight] = ???

  def calculate()= {

    val filteredOnDate: RDD[Flight] = filterOnDate()

    val (metric, newMetricStore) = getMetric(filteredOnDate)

    val resultAll = mergeMetric(metric, currentMetricStore)

    resultAll
  }
}

object TopAirportsByFlights {
  def apply(flights: RDD[Flight],
            currentStore: MetricStore) = new TopAirportsByFlights(flights, currentStore)
}
