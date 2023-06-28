package com.example
package schemas

import java.util.Date

case class Flight(
                    YEAR: Int,
                    MONTH: Int,
                    DAY: Int,
                    DAY_OF_WEEK: Int,
                    AIRLINE: String,
                    FLIGHT_NUMBER: Int,
                    TAIL_NUMBER: String,
                    ORIGIN_AIRPORT: String,
                    DESTINATION_AIRPORT: String,
                    SCHEDULED_DEPARTURE: String,
                    DEPARTURE_TIME: String,
                    DEPARTURE_DELAY: Int,
                    TAXI_OUT: Int,
                    WHEELS_OFF: String,
                    SCHEDULED_TIME: Int,
                    ELAPSED_TIME: Int,
                    AIR_TIME: Int,
                    DISTANCE: Int,
                    WHEELS_ON: String,
                    TAXI_IN: Int,
                    SCHEDULED_ARRIVAL: String,
                    ARRIVAL_TIME: String,
                    ARRIVAL_DELAY: Int,
                    DIVERTED: Int,
                    CANCELLED: Int,
                    CANCELLATION_REASON: String,
                    AIR_SYSTEM_DELAY: String,
                    SECURITY_DELAY: String,
                    AIRLINE_DELAY: String,
                    LATE_AIRCRAFT_DELAY: String,
                    WEATHER_DELAY: String,
                    NormalizeDate: Date
                 ) extends FromFile
