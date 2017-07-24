package geopyspark.geotrellis

import GeoTrellisUtils._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.vector._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._
import protos.tupleMessages.ProtoTuple

import scala.util.{Either, Left, Right}
import spray.json._


class ProjectedRasterRDD(val rdd: RDD[(ProjectedExtent, MultibandTile)]) extends RasterRDD[ProjectedExtent] {

  def collectMetadata(layout: Either[LayoutScheme, LayoutDefinition], crs: Option[CRS]): String = {
    (crs, layout) match {
      case (Some(crs), Right(layoutDefinition)) =>
        rdd.collectMetadata[SpatialKey](crs, layoutDefinition)
      case (None, Right(layoutDefinition)) =>
        rdd.collectMetadata[SpatialKey](layoutDefinition)
      case (Some(crs), Left(layoutScheme)) =>
        rdd.collectMetadata[SpatialKey](crs, layoutScheme)._2
      case (None, Left(layoutScheme)) =>
        rdd.collectMetadata[SpatialKey](layoutScheme)._2
    }
  }.toJson.compactPrint

  def tileToLayout(tileLayerMetadata: String, resampleMethod: ResampleMethod): TiledRasterRDD[SpatialKey] = {
    val md = tileLayerMetadata.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    new SpatialTiledRasterRDD(None, MultibandTileLayerRDD(rdd.tileToLayout(md, resampleMethod), md))
  }

  def tileToLayout(layoutDefinition: LayoutDefinition, resampleMethod: ResampleMethod): TiledRasterRDD[SpatialKey] = {
    val sms = RasterSummary.collect[ProjectedExtent, SpatialKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head

    val metadata = TileLayerMetadata[SpatialKey](
      sm.cellType,
      layoutDefinition,
      sm.extent,
      sm.crs,
      sm.bounds.setSpatialBounds(layoutDefinition.mapTransform(sm.extent))
    )

    SpatialTiledRasterRDD(None, MultibandTileLayerRDD(rdd.tileToLayout(metadata, resampleMethod), metadata))
  }

  def tileToLayout(layoutType: LayoutType, resampleMethod: ResampleMethod): TiledRasterRDD[SpatialKey] = {
    val sms = RasterSummary.collect[ProjectedExtent, SpatialKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head
    val (metadata, zoom) = sm.toTileLayerMetadata(layoutType)
    val tiled = rdd.tileToLayout(metadata, resampleMethod)
    new SpatialTiledRasterRDD(zoom, MultibandTileLayerRDD(tiled, metadata))
  }

  def reproject(targetCRS: String, resampleMethod: ResampleMethod): ProjectedRasterRDD = {
    val crs = TileRDD.getCRS(targetCRS).get
    new ProjectedRasterRDD(rdd.reproject(crs, resampleMethod))
  }

  def reproject(targetCRS: String, layoutType: LayoutType, resampleMethod: ResampleMethod): TiledRasterRDD[SpatialKey] = {
    val crs = TileRDD.getCRS(targetCRS).get
    val tiled = tileToLayout(LocalLayout(256), resampleMethod).rdd
    layoutType match {
      case GlobalLayout(tileSize, null, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (zoom, reprojected) = tiled.reproject(crs, scheme, resampleMethod)
        new SpatialTiledRasterRDD(Some(zoom), reprojected)

      case GlobalLayout(tileSize, zoom, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (_, reprojected) = tiled.reproject(crs, scheme.levelForZoom(zoom).layout, resampleMethod)
        new SpatialTiledRasterRDD(Some(zoom), reprojected)

      case LocalLayout(tileSize) =>
        val (_, reprojected) = tiled.reproject(crs, FloatingLayoutScheme(tileSize), resampleMethod)
        new SpatialTiledRasterRDD(None, reprojected)
    }
  }

  def reproject(
    target_crs: String,
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod
  ): TiledRasterRDD[SpatialKey] = {
    val tiled = tileToLayout(layoutDefinition, resampleMethod).rdd
    val (zoom, reprojected) = TileRDDReproject(tiled, TileRDD.getCRS(target_crs).get, Right(layoutDefinition), resampleMethod)
    SpatialTiledRasterRDD(Some(zoom), reprojected)
  }

  def reclassify(reclassifiedRDD: RDD[(ProjectedExtent, MultibandTile)]): RasterRDD[ProjectedExtent] =
    ProjectedRasterRDD(reclassifiedRDD)

  def reclassifyDouble(reclassifiedRDD: RDD[(ProjectedExtent, MultibandTile)]): RasterRDD[ProjectedExtent] =
    ProjectedRasterRDD(reclassifiedRDD)

  def withRDD(result: RDD[(ProjectedExtent, MultibandTile)]): RasterRDD[ProjectedExtent] =
    ProjectedRasterRDD(result)

  def toProtoRDD(): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(ProjectedExtent, MultibandTile), ProtoTuple](rdd)

  def toPngRDD(pngRDD: RDD[(ProjectedExtent, Array[Byte])]): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(ProjectedExtent, Array[Byte]), ProtoTuple](pngRDD)

  def toGeoTiffRDD(
    tags: Tags,
    geotiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]] = {
    val geotiffRDD = rdd.map { x =>
      (x._1, MultibandGeoTiff(x._2, x._1.extent, x._1.crs, tags, geotiffOptions).toByteArray)
    }

    PythonTranslator.toPython[(ProjectedExtent, Array[Byte]), ProtoTuple](geotiffRDD)
  }
}

object ProjectedRasterRDD {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): ProjectedRasterRDD =
    ProjectedRasterRDD(
      PythonTranslator.fromPython[
        (ProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(ProjectedExtent, MultibandTile)]): ProjectedRasterRDD =
    new ProjectedRasterRDD(rdd)
}
