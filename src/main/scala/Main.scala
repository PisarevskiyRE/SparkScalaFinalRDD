package com.example

import jobs.{Job, JobConfig, SessionWrapper}

import com.example.jobs.Reader.ReadConfig
import org.apache.log4j.{Level, Logger}

object FlightAnalyzer extends SessionWrapper{

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val job = Job(
      JobConfig(
        ReadConfig(
          hasHeader = true,
          separator = ','),
        //"src/main/resources/file_path.csv",
        args(0),
        //"src/main/resources/metrics_store.csv"
        args(1)
      )
    )
    job.run()
  }
}
