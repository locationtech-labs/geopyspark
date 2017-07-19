package geopyspark.geotrellis

import org.apache.log4j.Logger


object Log {
  val logger = Logger.getLogger(this.getClass)

  def debug(s: String): Unit = logger.debug(s)
  def info(s: String): Unit = logger.info(s)
  def warn(s: String): Unit = logger.warn(s)
  def error(s: String): Unit = logger.error(s)
}
