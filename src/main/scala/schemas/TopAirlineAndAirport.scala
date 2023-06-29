package com.example

import schemas.{FromFile, MetricResult}


case class TopAirlineAndAirport(airline: String, airport: String, count: Int) extends MetricResult with FromFile
