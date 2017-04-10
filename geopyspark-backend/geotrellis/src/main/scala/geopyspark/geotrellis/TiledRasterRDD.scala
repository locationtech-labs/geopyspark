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
  import Constants._

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
        case FLOAT => FloatingLayoutScheme(tileSize)
        case ZOOM => ZoomedLayoutScheme(_crs, tileSize, resolutionThreshold)
      }

    reproject(Left(layoutScheme), _crs, getReprojectOptions(resampleMethod))
  }

  protected def reproject(
    layout: Either[LayoutScheme, LayoutDefinition],
    crs: CRS,
    options: Reproject.Options
  ): TiledRasterRDD[_]

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    resampleMethod: String
  ): TiledRasterRDD[_]

  def isZoomedLayer(tileSize: Int): Boolean =
    (tileSize & (tileSize - 1)) == 0

  def pyramid(
    startZoom: Int,
    endZoom: Int,
    resampleMethod: String
  ): Array[_] // Array[TiledRasterRDD[_]]

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

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    resampleMethod: String
  ): TiledRasterRDD[SpatialKey] = {
    val method: ResampleMethod = TileRDD.getResampleMethod(resampleMethod)
    val mapKeyTransform =
      MapKeyTransform(
        layoutDefinition.extent,
        layoutDefinition.layoutCols,
        layoutDefinition.layoutRows)

    val crs = rdd.metadata.crs
    val projectedRDD = rdd.map{ x => (ProjectedExtent(mapKeyTransform(x._1), crs), x._2) }
    val retiledLayerMetadata = rdd.metadata.copy(
      layout = layoutDefinition,
      bounds = KeyBounds(mapKeyTransform(rdd.metadata.extent))
    )

    val tileLayer =
      MultibandTileLayerRDD(projectedRDD.tileToLayout(retiledLayerMetadata, method), retiledLayerMetadata)

    new SpatialTiledRasterRDD(None, tileLayer)
  }

  def pyramid(
    startZoom: Int,
    endZoom: Int,
    resampleMethod: String
  ): Array[TiledRasterRDD[SpatialKey]] = {
    require(! rdd.metadata.bounds.isEmpty, "Can not pyramid an empty RDD")

    val method: ResampleMethod = TileRDD.getResampleMethod(resampleMethod)
    val scheme = ZoomedLayoutScheme(rdd.metadata.crs, rdd.metadata.tileRows)
    val levelZoom = math.log(rdd.metadata.layoutRows.toDouble) / math.log(2)

    val pyramidBase: MultibandTileLayerRDD[SpatialKey] =
      if (isZoomedLayer(rdd.metadata.tileRows) && startZoom == levelZoom)
        rdd
      else {
        val LayoutLevel(_, layoutDefinition) = scheme.levelForZoom(startZoom)
        tileToLayout(layoutDefinition, resampleMethod).rdd
      }

    val leveledList =
      Pyramid.levelStream(
        pyramidBase,
        scheme,
        startZoom,
        endZoom,
        Pyramid.Options(resampleMethod=method)
      )

    leveledList.map{ x => new SpatialTiledRasterRDD(Some(x._1), x._2) }.toArray
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

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    resampleMethod: String
  ): TiledRasterRDD[SpaceTimeKey] = {
    val method: ResampleMethod = TileRDD.getResampleMethod(resampleMethod)
    val mapKeyTransform =
      MapKeyTransform(
        layoutDefinition.extent,
        layoutDefinition.layoutCols,
        layoutDefinition.layoutRows)

    val crs = rdd.metadata.crs

    val temporalRDD = rdd.map { x =>
      (TemporalProjectedExtent(mapKeyTransform(x._1), rdd.metadata.crs, x._1.instant), x._2)
    }

    val bounds = rdd.metadata.bounds.get
    val spatialBounds = KeyBounds(mapKeyTransform(rdd.metadata.extent))
    val retiledLayerMetadata = rdd.metadata.copy(
      layout = layoutDefinition,
      bounds = KeyBounds(
        minKey = bounds.minKey.setComponent[SpatialKey](spatialBounds.minKey),
        maxKey = bounds.maxKey.setComponent[SpatialKey](spatialBounds.maxKey)
      )
    )

    val tileLayer =
      MultibandTileLayerRDD(temporalRDD.tileToLayout(retiledLayerMetadata, method), retiledLayerMetadata)

    new TemporalTiledRasterRDD(None, tileLayer)
  }

  def pyramid(
    startZoom: Int,
    endZoom: Int,
    resampleMethod: String
  ): Array[TiledRasterRDD[SpaceTimeKey]] = {
    require(! rdd.metadata.bounds.isEmpty, "Can not pyramid an empty RDD")

    val method: ResampleMethod = TileRDD.getResampleMethod(resampleMethod)
    val scheme = ZoomedLayoutScheme(rdd.metadata.crs, rdd.metadata.tileRows)
    val levelZoom = math.log(rdd.metadata.layoutRows.toDouble) / math.log(2)

    val pyramidBase: MultibandTileLayerRDD[SpaceTimeKey] =
      if (isZoomedLayer(rdd.metadata.tileRows) && startZoom == levelZoom)
        rdd
      else {
        val LayoutLevel(_, layoutDefinition) = scheme.levelForZoom(startZoom)
        tileToLayout(layoutDefinition, resampleMethod).rdd
      }

    val leveledList =
      Pyramid.levelStream(
        pyramidBase,
        scheme,
        startZoom,
        endZoom,
        Pyramid.Options(resampleMethod=method)
      )

    leveledList.map{ x => new TemporalTiledRasterRDD(Some(x._1), x._2) }.toArray
  }
}
