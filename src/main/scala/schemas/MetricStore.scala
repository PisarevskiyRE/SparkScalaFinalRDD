package com.example
package schemas

import java.sql.Date

case class MetricStore(
                        metricName: String,
                        top: Int,
                        order: String,
                        date: Date,
                        dateFrom: Date,
                        dateTo: Date,
                        path: String,
                        pathAll: String
                      )  extends FromFile
