package com.example
package jobs


object Reader {
  case class ReadConfig(
                         hasHeader: Boolean = true,
                         separator: Char = ',')
}

case class JobConfig(
                      readerConfig: Reader.ReadConfig,
                      configPath: String,
                      storePath: String)
