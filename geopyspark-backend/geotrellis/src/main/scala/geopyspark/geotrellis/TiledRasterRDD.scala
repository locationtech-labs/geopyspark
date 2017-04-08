package geopyspark.geotrellis

import geopyspark.geotrellis.GeoTrellisUtils._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.raster._
import geotrellis.raster.io._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.avro._
import geotrellis.spark.tiling._

import spray.json._
import spray.json.DefaultJsonProtocol._

import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import scala.reflect._
import scala.collection.JavaConverters._


abstract class TiledRasterRDD[K: SpatialComponent: AvroRecordCodec: JsonFormat: ClassTag] extends TileRDD[K] {
  def rdd: RDD[(K, MultibandTile)] with Metadata[TileLayerMetadata[K]]
  def zoomLevel: Option[Int]

  def getZoom: Integer =
    zoomLevel match {
      case None => null
      case Some(z) => new Integer(z)
    }

  /** Encode RDD as Avro bytes and return it with avro schema used */
  def toAvroRDD(): (JavaRDD[Array[Byte]], String) = PythonTranslator.toPython(rdd)

  def layerMetadata: String = rdd.metadata.toJson.prettyPrint

  private def getReprojectOptions(resampleMethod: String): Reproject.Options = {
    import Reproject.Options

    val method = TileRDD.getResampleMethod(resampleMethod)

    Options(geotrellis.raster.reproject.Reproject.Options(method=method))
  }

  def reproject(
    extent: java.util.Map[String, Double],
    layout: java.util.Map[String, Int],
    crs: String,
    resampleMethod: String
  ): TiledRasterRDD[_] = {
    val layoutDefinition = Right(LayoutDefinition(extent.toExtent, layout.toTileLayout))

    reproject(layoutDefinition, TileRDD.getCRS(crs).get, getReprojectOptions(resampleMethod))
  }

  def reproject(
    scheme: String,
    tileSize: Int,
    resolutionThreshold: Double,
    crs: String,
    resampleMethod: String
  ): TiledRasterRDD[_] = {
    val _crs = TileRDD.getCRS(crs).get

    val layoutScheme =
      scheme match {
        case "float" => FloatingLayoutScheme(tileSize)
        case "zoom" => ZoomedLayoutScheme(_crs, tileSize, resolutionThreshold)
      }

    reproject(Left(layoutScheme), _crs, getReprojectOptions(resampleMethod))
  }

  def reproject(
    layout: Either[LayoutScheme, LayoutDefinition],
    crs: CRS,
    options: Reproject.Options
  ): TiledRasterRDD[_]
}


class SpatialTiledRasterRDD(
  val zoomLevel: Option[Int],
  val rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]
) extends TiledRasterRDD[SpatialKey] {

  def reproject(
    layout: Either[LayoutScheme, LayoutDefinition],
    crs: CRS,
    options: Reproject.Options
  ): TiledRasterRDD[SpatialKey] = {
    val (zoom, reprojected) = TileRDDReproject(rdd, crs, layout, options)
    new SpatialTiledRasterRDD(Some(zoom), reprojected)
  }

  def pyramid(
    tileSize: Int,
    resolutionThreshold: Double,
    startZoom: Int,
    endZoom: Int,
    resampleMethod: String
  ): java.util.List[SpatialTiledRasterRDD] = {
    val leveledList =
      Pyramid.levelStream(
        rdd,
        ZoomedLayoutScheme(rdd.metadata.crs, tileSize, resolutionThreshold),
        startZoom,
        endZoom,
        Pyramid.Options(resampleMethod=TileRDD.getResampleMethod(resampleMethod))
      )

    leveledList.map{ x => new SpatialTiledRasterRDD(Some(x._1), x._2) }.toList.asJava
  }
}


class TemporalTiledRasterRDD(
  val zoomLevel: Option[Int],
  val rdd: RDD[(SpaceTimeKey, MultibandTile)] with Metadata[TileLayerMetadata[SpaceTimeKey]]
) extends TiledRasterRDD[SpaceTimeKey] {

  def reproject(
    layout: Either[LayoutScheme, LayoutDefinition],
    crs: CRS,
    options: Reproject.Options
  ): TiledRasterRDD[SpaceTimeKey] = {
    val (zoom, reprojected) = TileRDDReproject(rdd, crs, layout, options)
    new TemporalTiledRasterRDD(Some(zoom), reprojected)
  }

  def pyramid(
    tileSize: Int,
    resolutionThreshold: Double,
    startZoom: Int,
    endZoom: Int,
    resampleMethod: String
  ): java.util.List[TemporalTiledRasterRDD] = {
    val leveledList =
      Pyramid.levelStream(
        rdd,
        ZoomedLayoutScheme(rdd.metadata.crs, tileSize, resolutionThreshold),
        startZoom,
        endZoom,
        Pyramid.Options(resampleMethod=TileRDD.getResampleMethod(resampleMethod))
      )

    leveledList.map{ x => new TemporalTiledRasterRDD(Some(x._1), x._2) }.toList.asJava
  }
}
