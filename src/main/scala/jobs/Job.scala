package com.example
package jobs

import com.example.readers.CsvReaderMetricStore.getMetricStoreByName
import com.example.readers.{CsvReaderAirline, CsvReaderAirport, CsvReaderFilePath, CsvReaderFlights, CsvReaderMetricStore}
import com.example.schemas.{Airline, Airport, FilePath, Flight, MetricStore}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

class Job(config: JobConfig) extends SessionWrapper {
  def run(): Unit = {

    val csvReaderFilePath = new CsvReaderFilePath(config.readerConfig)

    val configs: RDD[FilePath] = csvReaderFilePath.read(config.configPath)

    val airlinesFilePath = csvReaderFilePath.getPathByName(configs, "airlines")
    val airportsFilePath = csvReaderFilePath.getPathByName(configs, "airports")
    val flightsFilePath = csvReaderFilePath.getPathByName(configs, "flights")



    val airlineRDD: RDD[Airline] = CsvReaderAirline().read(airlinesFilePath)
    val airportsRDD: RDD[Airport] = CsvReaderAirport().read(airportsFilePath)
    val flightsRDD: RDD[Flight] = CsvReaderFlights().read(flightsFilePath)

//    airlineRDD.foreach(println)
//    airportsRDD.foreach(println)
//    flightsRDD.foreach(println)


    val initMetricStoreRDD = CsvReaderMetricStore().read(config.storePath)
//    initMetricStoreRDD.foreach(println)

    val topAirportsByFlightsMetricStore: MetricStore = getMetricStoreByName(initMetricStoreRDD, "TopAirportsByFlights")
//    println(topAirportsByFlightsMetricStore)

    val TopAirportsByFlights = metrics.TopAirportsByFlights(flightsRDD,  topAirportsByFlightsMetricStore).calculate()

    TopAirportsByFlights.collect().foreach(println)
  }
}

object Job {
  def apply(config: JobConfig) = new Job(config)
}
