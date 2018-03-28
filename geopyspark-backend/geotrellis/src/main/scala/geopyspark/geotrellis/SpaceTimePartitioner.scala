package geopyspark.geotrellis

import geotrellis.spark._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.zcurve._
import geotrellis.util._

import org.apache.spark._

import scala.reflect._


class SpaceTimePartitioner[K: SpatialComponent](
  partitions: Int,
  bits: Int,
  temporalType: String,
  temporalResolution: String
) extends Partitioner {
  val timeResolution: Long =
    (temporalType, temporalResolution) match {
      case ("millis", null) => 1.toLong
      case ("millis", r) => r.toLong
      case ("seconds", null) => 1000L
      case ("seconds", r) => 1000L * r.toLong
      case ("minutes", null) => 1000L * 60
      case ("minutes", r) => 1000L * 60 * r.toLong
      case ("hours", null) =>  1000L * 60 * 60
      case ("hours", r) =>  1000L * 60 * 60 * r.toLong
      case ("days", null) =>  1000L * 60 * 60 * 24
      case ("days", r) =>  1000L * 60 * 60 * 24 * r.toLong
      case ("weeks", null) =>  1000L * 60 * 60 * 24 * 7
      case ("weeks", r) =>  1000L * 60 * 60 * 24 * 7 * r.toLong
      case ("months", null) =>  1000L * 60 * 60 * 24 * 30
      case ("months", r) =>  1000L * 60 * 60 * 24 * 30 * r.toLong
      case ("year", null) =>  1000L * 60 * 60 * 24 * 365
      case ("year", r) =>  1000L * 60 * 60 * 24 * 365 * r.toLong
    }

  def numPartitions: Int = partitions

  def getBits: Int = bits

  def getTimeResolution =
    temporalResolution match {
      case null => null
      case s: String => s.toInt
    }

  def getTimeUnit: String = temporalType

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[SpaceTimeKey]
    val SpatialKey(col, row) = k.getComponent[SpatialKey]
    ((Z3(col, row, (k.instant / timeResolution).toInt).z >> bits) % partitions).toInt
  }
}

object SpaceTimePartitioner {
  def apply(
    partitions: Int,
    bits: Int,
    temporalType: String,
    temporalResolution: String
  ): SpaceTimePartitioner[SpaceTimeKey] =
    new SpaceTimePartitioner[SpaceTimeKey](partitions, bits, temporalType, temporalResolution)

  def apply(
    partitions: Int,
    temporalType: String,
    temporalResolution: String
  ): SpaceTimePartitioner[SpaceTimeKey] =
    new SpaceTimePartitioner[SpaceTimeKey](partitions, 8, temporalType, temporalResolution)
}
