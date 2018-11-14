package geopyspark.geotrellis.vlm

import geopyspark.geotrellis.{PartitionStrategy, ProjectedRasterLayer, SpatialTiledRasterLayer}
import geopyspark.geotrellis.{LayoutType => GPSLayoutType, LocalLayout => GPSLocalLayout, GlobalLayout => GPSGlobalLayout, SpatialTiledRasterLayer}

import geopyspark.geotrellis.Constants.{GEOTRELLIS, GDAL}

import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.contrib.vlm.gdal.GDALRasterSource

import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.util._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


object RasterSource {
  def read(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[String],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): ProjectedRasterLayer =
    read(
      sc,
      layerType,
      sc.parallelize(paths.asScala),
      targetCRS,
      resampleMethod,
      readMethod
    )

  def readOrdered(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[java.util.Map[String, String]],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy,
    readMethod: String
  ): ProjectedRasterLayer = {
    val scalaPaths: Seq[Seq[(String, String)]] = paths.asScala.toSeq.map { _.asScala.toSeq }

    val formattedPaths: Seq[((String, String), String)] =
      scalaPaths.flatMap { mappedPaths =>
        mappedPaths.map { case (k, v) => ((k, v), v) }
      }

    val rdd: RDD[((String, String), String)] = sc.parallelize(formattedPaths, formattedPaths.size)

    val rasterSourceRDD: RDD[((String, String), RasterSource)] =
      (readMethod match {
        case GEOTRELLIS => rdd.mapValues { new GeoTiffRasterSource(_): RasterSource }
        case GDAL => rdd.mapValues { GDALRasterSource(_): RasterSource }
      }).cache()

    val reprojectedSourcesRDD: RDD[((String, String), RasterSource)] =
      targetCRS match {
        case crs: String =>
          rasterSourceRDD.mapValues { _.reproject(CRS.fromString(crs), resampleMethod) }
        case null =>
          rasterSourceRDD
      }

    ???
  }

  def read(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[String],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): ProjectedRasterLayer = {
    val rasterSourceRDD: RDD[RasterSource] =
      (readMethod match {
        case GEOTRELLIS => rdd.map { GeoTiffRasterSource(_): RasterSource }
        case GDAL => rdd.map { GDALRasterSource(_): RasterSource }
      }).cache()

    val reprojectedSourcesRDD: RDD[RasterSource] =
      targetCRS match {
        case crs: String =>
          rasterSourceRDD.map { _.reproject(CRS.fromString(crs), resampleMethod) }
        case null =>
          rasterSourceRDD
      }

    val projectedRasterRDD: RDD[(ProjectedExtent, MultibandTile)] =
      reprojectedSourcesRDD.flatMap { source: RasterSource =>
        source.read(source.extent) match {
          case Some(raster) => Some((ProjectedExtent(raster.extent, source.crs), raster.tile))
          case None => None
        }
      }

    rasterSourceRDD.unpersist()

    ProjectedRasterLayer(projectedRasterRDD)
  }

  def readToLayout(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[String],
    layoutType: GPSLayoutType,
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): SpatialTiledRasterLayer =
    readToLayout(
      sc,
      layerType,
      sc.parallelize(paths.asScala),
      layoutType,
      targetCRS,
      resampleMethod,
      readMethod
    )

  def readToLayout(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[String],
    layoutType: LayoutType,
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): SpatialTiledRasterLayer = {
    // TODO: These are the things that still need to be done:
    // 1. Support TemporalTiledRasterLayer (ie. generic K)
    // 2. Use the partitionStrategy parameter

    val rasterSourceRDD: RDD[RasterSource] =
      (readMethod match {
        case GEOTRELLIS => rdd.map { new GeoTiffRasterSource(_): RasterSource }
        case GDAL => rdd.map { GDALRasterSource(_): RasterSource }
      }).cache()

    val reprojectedSourcesRDD: RDD[RasterSource] =
      targetCRS match {
        case crs: String =>
          rasterSourceRDD.map { _.reproject(CRS.fromString(crs), resampleMethod) }
        case null =>
          rasterSourceRDD
      }

    val metadata: RasterSummary = RasterSummary.fromRDD(reprojectedSourcesRDD)

    val LayoutLevel(zoom, layout) =
      layoutType match {
        case global: GlobalLayout =>
          val scheme = ZoomedLayoutScheme(metadata.crs, global.tileSize)
          scheme.levelForZoom(global.zoom)
        case local: LocalLayout =>
          val scheme = FloatingLayoutScheme(local.tileCols, local.tileRows)
          metadata.levelFor(scheme)
      }

    val layoutRDD: RDD[LayoutTileSource] = reprojectedSourcesRDD.map { _.tileToLayout(layout, resampleMethod) }

    val tileLayerMetadata: TileLayerMetadata[SpatialKey] =
      metadata.toTileLayerMetadata(layout, zoom)._1

    val tiledRDD: RDD[(SpatialKey, MultibandTile)] =
      layoutRDD.flatMap { _.readAll() }

    rasterSourceRDD.unpersist()

    val contextRDD: MultibandTileLayerRDD[SpatialKey] =
      ContextRDD(tiledRDD, tileLayerMetadata)

    SpatialTiledRasterLayer(zoom.toInt, contextRDD)
  }

  implicit def gps2VLM(layoutType: GPSLayoutType): LayoutType =
    layoutType match {
      case local: GPSLocalLayout => LocalLayout(local.tileCols, local.tileRows)
      case global: GPSGlobalLayout => GlobalLayout(global.tileSize, global.zoom, global.threshold)
    }
}
