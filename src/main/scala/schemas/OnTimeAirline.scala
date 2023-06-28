package com.example
package schemas

case class OnTimeAirline(airline: String,
                          count: Int) extends MetricResult with FromFile
