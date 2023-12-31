package com.example
package jobs

import com.example.metrics._
import com.example.readers.CsvReaderFromFileMetricStore.getMetricStoreByName
import com.example.readers.{CsvReaderFromFileAirline, CsvReaderFromFileAirport, CsvReaderFromFileFilePath, CsvReaderFromFileFlights, CsvReaderFromFileMetricStore}
import com.example.schemas.{Airline, Airport, FilePath, Flight, MetricStore, TopAirportByFlight}
import com.example.writers.{CsvWriterMetricDelayPercent, CsvWriterMetricDelayReasons, CsvWriterMetricFlightByDayOfWeek, CsvWriterMetricOnTimeAirline, CsvWriterMetricStore, CsvWriterMetricTopAirlineAndAirport, CsvWriterMetricTopAirportByFlight}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

class Job(config: JobConfig) extends SessionWrapper {
  def run(): Unit = {

    val configs: RDD[FilePath] = CsvReaderFromFileFilePath.csvReaderFromFileFilePath.read(config.configPath)

    val airlinesFilePath = CsvReaderFromFileFilePath.getPathByName(configs, "airlines")
    val airportsFilePath = CsvReaderFromFileFilePath.getPathByName(configs, "airports")
    val flightsFilePath = CsvReaderFromFileFilePath.getPathByName(configs, "flights")



    val airlineRDD: RDD[Airline] = CsvReaderFromFileAirline.csvReaderFromFileAirline.read(airlinesFilePath)
    val airportsRDD: RDD[Airport] = CsvReaderFromFileAirport.csvReaderFromFileAirport.read(airportsFilePath)
    val flightsRDD: RDD[Flight] = CsvReaderFromFileFlights.csvReaderFromFileFlight.read(flightsFilePath)

    val initMetricStoreRDD = CsvReaderFromFileMetricStore.csvReaderFromFileMetricStore.read(config.storePath)



    /**
     * 1
     */


    val topAirportsByFlightsMetricStore: MetricStore = getMetricStoreByName(initMetricStoreRDD, "TopAirportsByFlights")
    val (topAirportsByFlightsAll ,topAirportsByFlights ,newTopAirportsByFlightsMetricStore) = metrics.TopAirportsByFlights(flightsRDD,  topAirportsByFlightsMetricStore).calculate()



    CsvWriterMetricTopAirportByFlight.csvWriterMetricOnTimeAirline.write(topAirportsByFlights, newTopAirportsByFlightsMetricStore.path)
    CsvWriterMetricTopAirportByFlight.csvWriterMetricOnTimeAirline.write(topAirportsByFlightsAll, newTopAirportsByFlightsMetricStore.pathAll)

    /**
     * 2
     */

    val onTimeAirlinesMetricStore: MetricStore = getMetricStoreByName(initMetricStoreRDD, "OnTimeAirlines")
    val (onTimeAirlinesAll, onTimeAirlines, newOnTimeAirlinesMetricStore) = metrics.OnTimeAirlines(flightsRDD, onTimeAirlinesMetricStore).calculate()


    CsvWriterMetricOnTimeAirline.csvWriterMetricOnTimeAirline.write(onTimeAirlines, newOnTimeAirlinesMetricStore.path)
    CsvWriterMetricOnTimeAirline.csvWriterMetricOnTimeAirline.write(onTimeAirlinesAll, newOnTimeAirlinesMetricStore.pathAll)

    /**
     * 3
     */

    val topArilineAndAirportMetricStore: MetricStore = getMetricStoreByName(initMetricStoreRDD, "TopAirlinesAndAirports")
    val (topArilineAndAirportAll, topAirlineAndAirport, newTopAirlineAndAirportMetricStore) = metrics.TopAirlinesAndAirports(flightsRDD, topArilineAndAirportMetricStore).calculate()


    CsvWriterMetricTopAirlineAndAirport.csvWriterMetricTopArlineAndAirport.write(topAirlineAndAirport, newTopAirlineAndAirportMetricStore.path)
    CsvWriterMetricTopAirlineAndAirport.csvWriterMetricTopArlineAndAirport.write(topArilineAndAirportAll, newTopAirlineAndAirportMetricStore.pathAll)

    /**
     * 4
     */

    val flightsByDayOfWeekMetricStore: MetricStore = getMetricStoreByName(initMetricStoreRDD, "FlightsByDayOfWeek")
    val (flightsByDayOfWeekAll, flightsByDayOfWeek, newFlightsByDayOfWeekMetricStore) = metrics.FlightsByDayOfWeek(flightsRDD, flightsByDayOfWeekMetricStore).calculate()


    CsvWriterMetricFlightByDayOfWeek.csvWriterMetricFlightByDayOfWeek.write(flightsByDayOfWeek, newFlightsByDayOfWeekMetricStore.path)
    CsvWriterMetricFlightByDayOfWeek.csvWriterMetricFlightByDayOfWeek.write(flightsByDayOfWeekAll, newFlightsByDayOfWeekMetricStore.pathAll)

    /**
     * 5
     */
    val delayReasonsMetricStore: MetricStore = getMetricStoreByName(initMetricStoreRDD, "DelayReasons")
    val (delayReasonsAll, delayReasons, newDelayReasonsMetricStore) = metrics.DelayReasons(flightsRDD, delayReasonsMetricStore).calculate()


    CsvWriterMetricDelayReasons.csvWriterMetricDelayReasons.write(delayReasons, newDelayReasonsMetricStore.path)
    CsvWriterMetricDelayReasons.csvWriterMetricDelayReasons.write(delayReasonsAll, newDelayReasonsMetricStore.pathAll)

    /**
     * 6
     */

    val delayPercentsMetricStore: MetricStore = getMetricStoreByName(initMetricStoreRDD, "DelayPercents")
    val (delayPercentsAll, delayPercents, newDelayPercentsMetricStore) = metrics.DelayPercents(flightsRDD, delayPercentsMetricStore).calculate()


    CsvWriterMetricDelayPercent.csvWriterMetricDelayPercent.write(delayPercents, newDelayPercentsMetricStore.path)
    CsvWriterMetricDelayPercent.csvWriterMetricDelayPercent.write(delayPercentsAll, newDelayPercentsMetricStore.pathAll)

    CsvWriterMetricStore.csvWriterMetricStore.write(
      spark.sparkContext.parallelize(Seq(
        newTopAirportsByFlightsMetricStore,
        newOnTimeAirlinesMetricStore,
        newTopAirlineAndAirportMetricStore,
        newFlightsByDayOfWeekMetricStore,
        newDelayReasonsMetricStore,
        newDelayPercentsMetricStore
      )
    ), config.storePath
    )
  }
}

object Job {
  def apply(config: JobConfig) = new Job(config)
}
