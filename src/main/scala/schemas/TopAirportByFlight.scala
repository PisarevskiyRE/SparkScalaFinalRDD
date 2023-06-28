package com.example
package schemas

case class TopAirportByFlight(
                               airport: String,
                               count: Int
                             ) extends MetricResult with FromFile
