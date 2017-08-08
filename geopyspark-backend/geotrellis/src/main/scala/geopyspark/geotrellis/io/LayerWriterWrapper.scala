package geopyspark.geotrellis.io

import geopyspark.geotrellis._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hbase._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.hilbert._
import geotrellis.spark.io.s3._
import geotrellis.vector._

import spray.json._

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime

import geopyspark.geotrellis.PythonTranslator


/**
  * Base wrapper class for all backends that provide a
  * LayerWriter[LayerId].
  */
class LayerWriterWrapper(attributeStore: AttributeStore, uri: String) {
  val layerWriter: LayerWriter[LayerId] = LayerWriter(attributeStore, uri)

  private def getSpatialIndexMethod(indexStrategy: String): KeyIndexMethod[SpatialKey] =
    indexStrategy match {
      case "zorder" => ZCurveKeyIndexMethod
      case "hilbert" => HilbertKeyIndexMethod
      case "rowmajor" => RowMajorKeyIndexMethod
      case _ => throw new Exception
    }

  private def getTemporalIndexMethod(
    timeString: String,
    indexStrategy: String
  ) =
    (indexStrategy, timeString) match {
      case ("zorder", "millis") => ZCurveKeyIndexMethod.byMilliseconds(1)
      case ("zorder", "seconds") => ZCurveKeyIndexMethod.bySecond
      case ("zorder", "minutes") => ZCurveKeyIndexMethod.byMinute
      case ("zorder", "hours") => ZCurveKeyIndexMethod.byHour
      case ("zorder", "days") => ZCurveKeyIndexMethod.byDay
      case ("zorder", "months") => ZCurveKeyIndexMethod.byMonth
      case ("zorder", "years") => ZCurveKeyIndexMethod.byYear
      case ("hilbert", _) => {
        timeString.split(",") match {
          case Array(minDate, maxDate, resolution) =>
            HilbertKeyIndexMethod(ZonedDateTime.parse(minDate),
              ZonedDateTime.parse(maxDate),
              resolution.toInt)
          case Array(resolution) =>
            HilbertKeyIndexMethod(resolution.toInt)
          case _ => throw new Exception(s"Invalid timeString: $timeString")
        }
      }
      case _ => throw new Exception
    }

  def writeSpatial(
    layerName: String,
    spatialRDD: TiledRasterLayer[SpatialKey],
    indexStrategy: String
  ): Unit = {
    val id =
      spatialRDD.zoomLevel match {
        case Some(zoom) => LayerId(layerName, zoom)
        case None => LayerId(layerName, 0)
      }
    val indexMethod = getSpatialIndexMethod(indexStrategy)
    layerWriter.write(id, spatialRDD.rdd, indexMethod)
  }

  def writeTemporal(
    layerName: String,
    temporalRDD: TiledRasterLayer[SpaceTimeKey],
    timeString: String,
    indexStrategy: String
  ): Unit = {
    val id =
      temporalRDD.zoomLevel match {
        case Some(zoom) => LayerId(layerName, zoom)
        case None => LayerId(layerName, 0)
      }
    val indexMethod = getTemporalIndexMethod(timeString, indexStrategy)
    layerWriter.write(id, temporalRDD.rdd, indexMethod)
  }
}
