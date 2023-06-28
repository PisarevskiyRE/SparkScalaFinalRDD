package com.example
package jobs

import com.example.metrics._
import com.example.readers.CsvReaderFromFileMetricStore.getMetricStoreByName
import com.example.readers.{CsvReaderFromFileAirline, CsvReaderFromFileAirport, CsvReaderFromFileFilePath, CsvReaderFromFileFlights, CsvReaderFromFileMetricStore}
import com.example.schemas.{Airline, Airport, FilePath, Flight, MetricStore, TopAirportByFlight}
import com.example.writers.{CsvWriterMetricOnTimeAirline, CsvWriterMetricTopAirportByFlight}
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

  }
}

object Job {
  def apply(config: JobConfig) = new Job(config)
}
