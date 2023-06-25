package com.example
package schemas


// IATA_CODE,
// AIRPORT,
// CITY,
// STATE,
// COUNTRY,
// LATITUDE,
// LONGITUDE
case class Airport(
                    IATA_CODE: String,
                    AIRPORT: String,
                    CITY: String,
                    STATE: String,
                    COUNTRY: String,
                    LATITUDE: Double,
                    LONGITUDE: Double
                  )
