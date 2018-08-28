package geopyspark.geotrellis.io

import geopyspark.geotrellis._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.cog._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.hilbert._
import geotrellis.spark.io.s3._
import geotrellis.vector._

import spray.json._

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime


/**
  * Base wrapper class for all backends that provide a
  * LayerWriter[LayerId].
  */
class LayerWriterWrapper(attributeStore: AttributeStore, uri: String, useCOGs: Boolean = false) {
  val layerWriter: Either[COGLayerWriter, LayerWriter[LayerId]] =
    if (useCOGs)
      Left(COGLayerWriter(attributeStore, uri))
    else
      Right(LayerWriter(attributeStore, uri))

  private def getSpatialIndexMethod(indexStrategy: String): KeyIndexMethod[SpatialKey] =
    indexStrategy match {
      case "zorder" => ZCurveKeyIndexMethod
      case "hilbert" => HilbertKeyIndexMethod
      case "rowmajor" => RowMajorKeyIndexMethod
      case _ => throw new Exception
    }

  private def getTemporalIndexMethod(
    timeString: String,
    timeResolution: String,
    indexStrategy: String
  ) =
    (indexStrategy, timeString, timeResolution) match {
      case ("zorder", "millis", null) => ZCurveKeyIndexMethod.byMilliseconds(1)
      case ("zorder", "millis", r) => ZCurveKeyIndexMethod.byMilliseconds(r.toLong)
      case ("zorder", "seconds", null) => ZCurveKeyIndexMethod.bySecond
      case ("zorder", "seconds", r) => ZCurveKeyIndexMethod.bySeconds(r.toInt)
      case ("zorder", "minutes", null) => ZCurveKeyIndexMethod.byMinute
      case ("zorder", "minutes", r) => ZCurveKeyIndexMethod.byMinutes(r.toInt)
      case ("zorder", "hours", null) => ZCurveKeyIndexMethod.byHour
      case ("zorder", "hours", r) => ZCurveKeyIndexMethod.byHours(r.toInt)
      case ("zorder", "days", null) => ZCurveKeyIndexMethod.byDay
      case ("zorder", "days", r) => ZCurveKeyIndexMethod.byDays(r.toInt)
      case ("zorder", "weeks", null) => ZCurveKeyIndexMethod.byDays(7)
      case ("zorder", "weeks", r) => ZCurveKeyIndexMethod.byDays(r.toInt * 7)
      case ("zorder", "months", null) => ZCurveKeyIndexMethod.byMonth
      case ("zorder", "months", r) => ZCurveKeyIndexMethod.byMonths(r.toInt)
      case ("zorder", "years", null) => ZCurveKeyIndexMethod.byYear
      case ("zorder", "years", r) => ZCurveKeyIndexMethod.byYears(r.toInt)
      case ("hilbert", _, _) => {
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

  private def getLayerId(layerName: String, layer: TiledRasterLayer[_]): LayerId =
    layer.zoomLevel match {
      case Some(zoom) => LayerId(layerName, zoom)
      case None => LayerId(layerName, 0)
    }

  def writeSpatial(
    layerName: String,
    spatialRDD: TiledRasterLayer[SpatialKey],
    indexStrategy: String
  ): Unit = {
    val indexMethod = getSpatialIndexMethod(indexStrategy)
    layerWriter match {
      case Left(cogWriter) =>
        val zoom = spatialRDD.zoomLevel.getOrElse(0)
        cogWriter.write(layerName, spatialRDD.rdd, zoom, indexMethod)
      case Right(avroWriter) =>
        val id =
          spatialRDD.zoomLevel match {
            case Some(zoom) => LayerId(layerName, zoom)
            case None => LayerId(layerName, 0)
          }
        avroWriter.write(id, spatialRDD.rdd, indexMethod)
    }
  }

  def writeTemporal(
    layerName: String,
    temporalRDD: TiledRasterLayer[SpaceTimeKey],
    timeString: String,
    timeResolution: String,
    indexStrategy: String
  ): Unit = {
    val indexMethod = getTemporalIndexMethod(timeString, timeResolution, indexStrategy)
    layerWriter match {
      case Left(cogWriter) =>
        val zoom = temporalRDD.zoomLevel.getOrElse(0)
        cogWriter.write(layerName, temporalRDD.rdd, zoom, indexMethod)
      case Right(avroWriter) =>
        val id =
          temporalRDD.zoomLevel match {
            case Some(zoom) => LayerId(layerName, zoom)
            case None => LayerId(layerName, 0)
          }
        avroWriter.write(id, temporalRDD.rdd, indexMethod)
    }
  }

  def updateSpatial(
    layerName: String,
    spatialRDD: TiledRasterLayer[SpatialKey]
  ): Unit =
    layerWriter match {
      case Left(cogWriter) =>
        val id = getLayerId(layerName, spatialRDD)
        cogWriter.update[SpatialKey, MultibandTile](id.name, spatialRDD.rdd, id.zoom, None)
      case Right(avroWriter) => avroWriter.update(getLayerId(layerName, spatialRDD), spatialRDD.rdd)
    }

  def updateTemporal(
    layerName: String,
    temporalRDD: TiledRasterLayer[SpaceTimeKey]
  ): Unit =
    layerWriter match {
      case Left(cogWriter) =>
        val id = getLayerId(layerName, temporalRDD)
        cogWriter.update[SpaceTimeKey, MultibandTile](id.name, temporalRDD.rdd, id.zoom, None)
      case Right(avroWriter) => avroWriter.update(getLayerId(layerName, temporalRDD), temporalRDD.rdd)
    }
}
