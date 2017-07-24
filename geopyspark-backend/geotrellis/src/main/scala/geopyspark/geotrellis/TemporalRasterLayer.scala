package geopyspark.geotrellis

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition, LayoutScheme, ZoomedLayoutScheme}
import geotrellis.spark.reproject._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import protos.tupleMessages.ProtoTuple
import spray.json._

import scala.util.{Either, Left, Right}

class TemporalRasterLayer(val rdd: RDD[(TemporalProjectedExtent, MultibandTile)]) extends RasterLayer[TemporalProjectedExtent] {

  def collectMetadata(layout: Either[LayoutScheme, LayoutDefinition], crs: Option[CRS]): String = {
    (crs, layout) match {
      case (Some(crs), Right(layoutDefinition)) =>
        rdd.collectMetadata[SpaceTimeKey](crs, layoutDefinition)
      case (None, Right(layoutDefinition)) =>
        rdd.collectMetadata[SpaceTimeKey](layoutDefinition)
      case (Some(crs), Left(layoutScheme)) =>
        rdd.collectMetadata[SpaceTimeKey](crs, layoutScheme)._2
      case (None, Left(layoutScheme)) =>
        rdd.collectMetadata[SpaceTimeKey](layoutScheme)._2
    }
  }.toJson.compactPrint

  def tileToLayout(layerMetadata: String, resampleMethod: ResampleMethod): TiledRasterLayer[SpaceTimeKey] = {
    val md = layerMetadata.parseJson.convertTo[TileLayerMetadata[SpaceTimeKey]]
    new TemporalTiledRasterLayer(None, MultibandTileLayerRDD(rdd.tileToLayout(md, resampleMethod), md))
  }

  def tileToLayout(layoutDefinition: LayoutDefinition, resampleMethod: ResampleMethod): TiledRasterLayer[SpaceTimeKey] = {
    val sms = RasterSummary.collect[TemporalProjectedExtent, SpaceTimeKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head

    val metadata = TileLayerMetadata[SpaceTimeKey](
      sm.cellType,
      layoutDefinition,
      sm.extent,
      sm.crs,
      sm.bounds.setSpatialBounds(layoutDefinition.mapTransform(sm.extent))
    )

    TemporalTiledRasterLayer(None, MultibandTileLayerRDD(rdd.tileToLayout(metadata, resampleMethod), metadata))
  }

  def tileToLayout(layoutType: LayoutType, resampleMethod: ResampleMethod): TiledRasterLayer[SpaceTimeKey] ={
    val sms = RasterSummary.collect[TemporalProjectedExtent, SpaceTimeKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head
    val (metadata, zoom) = sm.toTileLayerMetadata(layoutType)
    val tiled = rdd.tileToLayout(metadata, resampleMethod)
    new TemporalTiledRasterLayer(zoom, MultibandTileLayerRDD(tiled, metadata))
  }

  def reproject(targetCRS: String, resampleMethod: ResampleMethod): TemporalRasterLayer = {
    val crs = TileLayer.getCRS(targetCRS).get
    new TemporalRasterLayer(rdd.reproject(crs, resampleMethod))
  }

  def reproject(targetCRS: String, layoutType: LayoutType, resampleMethod: ResampleMethod): TiledRasterLayer[SpaceTimeKey] = {
    val crs = TileLayer.getCRS(targetCRS).get
    val tiled = tileToLayout(LocalLayout(256), resampleMethod).rdd
    layoutType match {
      case GlobalLayout(tileSize, null, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (zoom, reprojected) = tiled.reproject(crs, scheme, resampleMethod)
        new TemporalTiledRasterLayer(Some(zoom), reprojected)

      case GlobalLayout(tileSize, zoom, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (_, reprojected) = tiled.reproject(crs, scheme.levelForZoom(zoom).layout, resampleMethod)
        new TemporalTiledRasterLayer(Some(zoom), reprojected)

      case LocalLayout(tileSize) =>
        val (_, reprojected) = tiled.reproject(crs, FloatingLayoutScheme(tileSize), resampleMethod)
        new TemporalTiledRasterLayer(None, reprojected)
    }
  }

  def reproject(
    targetCRS: String,
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod
  ): TiledRasterLayer[SpaceTimeKey] = {
    val tiled = tileToLayout(layoutDefinition, resampleMethod).rdd
    val (zoom, reprojected) = TileRDDReproject(tiled, TileLayer.getCRS(targetCRS).get, Right(layoutDefinition), resampleMethod)
    TemporalTiledRasterLayer(Some(zoom), reprojected)
  }

  def reclassify(reclassifiedRDD: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterLayer[TemporalProjectedExtent] =
    TemporalRasterLayer(reclassifiedRDD)

  def reclassifyDouble(reclassifiedRDD: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterLayer[TemporalProjectedExtent] =
    TemporalRasterLayer(reclassifiedRDD)

  def withRDD(result: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterLayer[TemporalProjectedExtent] =
    TemporalRasterLayer(result)

  def toProtoRDD(): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(TemporalProjectedExtent, MultibandTile), ProtoTuple](rdd)

  def toPngRDD(pngRDD: RDD[(TemporalProjectedExtent, Array[Byte])]): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(TemporalProjectedExtent, Array[Byte]), ProtoTuple](pngRDD)

  def toGeoTiffRDD(
    tags: Tags,
    geotiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]] = {
    val geotiffRDD = rdd.map { x =>
      (x._1, MultibandGeoTiff(x._2, x._1.extent, x._1.crs, tags, geotiffOptions).toByteArray)
    }

    PythonTranslator.toPython[(TemporalProjectedExtent, Array[Byte]), ProtoTuple](geotiffRDD)
  }
}


object TemporalRasterLayer {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): TemporalRasterLayer =
    TemporalRasterLayer(
      PythonTranslator.fromPython[
        (TemporalProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(TemporalProjectedExtent, MultibandTile)]): TemporalRasterLayer =
    new TemporalRasterLayer(rdd)
}
