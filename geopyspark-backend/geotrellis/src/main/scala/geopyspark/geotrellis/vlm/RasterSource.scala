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

  def read(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[String],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy,
    readMethod: String
  ): ProjectedRasterLayer = {
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

  def readOrdered(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[java.util.Map[String, String]],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy,
    readMethod: String
  ): ProjectedRasterLayer = {
    val scalaPaths: Seq[(String, String)] = paths.asScala.toSeq.flatMap { _.asScala.toSeq }

    readOrdered(
      sc,
      layerType,
      sc.parallelize(scalaPaths, scalaPaths.size),
      targetCRS,
      resampleMethod,
      partitionStrategy,
      readMethod
    )
  }

  def readOrdered(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[(String, String)],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy,
    readMethod: String
  ): ProjectedRasterLayer = {
    val rasterSourceRDD: RDD[(String, (String, RasterSource))] =
      (readMethod match {
        case GEOTRELLIS =>
          rdd.mapPartitions { iter =>
            iter.map { case (k, v) =>
            (v, (k, new GeoTiffRasterSource(v): RasterSource))
            }
          }
        case GDAL =>
          rdd.mapPartitions { iter =>
            iter.map { case (k, v) =>
              (v, (k, GDALRasterSource(v): RasterSource))
            }
          }
      }).cache()

    val combinedRasterSourcesRDD: RDD[(String, Seq[(String, RasterSource)])] =
      rasterSourceRDD.combineByKey(
        (value: (String, RasterSource)) =>
          Seq(value),
        (rasterSeq: Seq[(String, RasterSource)], value: (String, RasterSource)) =>
          value +: rasterSeq,
        (s1: Seq[(String, RasterSource)], s2: Seq[(String, RasterSource)]) =>
          s1 ++: s2
        )

    val orderedRasterSourcesRDD: RDD[Seq[RasterSource]] =
      combinedRasterSourcesRDD.map { case (_, v) =>
        v.sortBy { _._1 }.map { case (_, source) => source }
      }

    val reprojectedSourcesRDD: RDD[Seq[RasterSource]] =
      targetCRS match {
        case crs: String =>
          orderedRasterSourcesRDD.map {
            _.map { _.reproject(CRS.fromString(crs), resampleMethod) }
          }
        case null =>
          orderedRasterSourcesRDD
      }

    val projectedRasterRDD: RDD[(ProjectedExtent, MultibandTile)] =
      reprojectedSourcesRDD.map { sources: Seq[RasterSource] =>
        val extent: Extent = sources.head.extent
        val crs: CRS = sources.head.crs

        val tiles: Seq[MultibandTile] =
          sources.flatMap { source: RasterSource =>
            source.read(source.extent) match {
              case Some(raster) => Some(raster.tile)
              case None => None
            }
          }

        (ProjectedExtent(extent, crs), MultibandTile(tiles.map { _.band(0) }))
      }

    rasterSourceRDD.unpersist()

    ProjectedRasterLayer(projectedRasterRDD)
  }

  def readOrderedToLayout(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[java.util.Map[String, String]],
    layoutType: LayoutType,
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): SpatialTiledRasterLayer = {
    val scalaPaths: Seq[(String, String)] = paths.asScala.toSeq.flatMap { _.asScala.toSeq }

    val rdd: RDD[(String, String)] = sc.parallelize(scalaPaths, scalaPaths.size)

    val rasterSourceRDD: RDD[(String, (String, RasterSource))] =
      (readMethod match {
        case GEOTRELLIS =>
          rdd.mapPartitions { iter =>
            iter.map { case (k, v) =>
            (v, (k, new GeoTiffRasterSource(v): RasterSource))
            }
          }
        case GDAL =>
          rdd.mapPartitions { iter =>
            iter.map { case (k, v) =>
              (v, (k, GDALRasterSource(v): RasterSource))
            }
          }
      }).cache()

    val combinedRasterSourcesRDD: RDD[(String, Seq[(String, RasterSource)])] =
      rasterSourceRDD.combineByKey(
        (value: (String, RasterSource)) =>
          Seq(value),
        (rasterSeq: Seq[(String, RasterSource)], value: (String, RasterSource)) =>
          value +: rasterSeq,
        (s1: Seq[(String, RasterSource)], s2: Seq[(String, RasterSource)]) =>
          s1 ++: s2
        )

    val orderedRasterSourcesRDD: RDD[Seq[RasterSource]] =
      combinedRasterSourcesRDD.map { case (_, v) =>
        v.sortBy { _._1 }.map { case (_, source) => source }
      }

    val reprojectedSourcesRDD: RDD[Seq[RasterSource]] =
      targetCRS match {
        case crs: String =>
          orderedRasterSourcesRDD.map {
            _.map { _.reproject(CRS.fromString(crs), resampleMethod) }
          }
        case null =>
          orderedRasterSourcesRDD
      }

    val rasterSummary: RasterSummary =
      reprojectedSourcesRDD
        .map { sources: Seq[RasterSource] =>
          sources
            .map { source =>
              val ProjectedExtent(extent, crs) = source.getComponent[ProjectedExtent]
              val cellSize = CellSize(extent, source.cols, source.rows)
              RasterSummary(crs, source.cellType, cellSize, extent, source.size, 1)
            }.reduce { _ combine _ }
        }
        .reduce { _ combine _ }

    val LayoutLevel(zoom, layout) =
      layoutType match {
        case global: GlobalLayout =>
          val scheme = ZoomedLayoutScheme(rasterSummary.crs, global.tileSize)
          scheme.levelForZoom(global.zoom)
        case local: LocalLayout =>
          val scheme = FloatingLayoutScheme(local.tileCols, local.tileRows)
          rasterSummary.levelFor(scheme)
      }

    val metadata: TileLayerMetadata[SpatialKey] =
      rasterSummary.toTileLayerMetadata(layout, zoom)._1

    val layoutsRDD: RDD[Seq[LayoutTileSource]] =
      reprojectedSourcesRDD.map { _.map { _.tileToLayout(layout, resampleMethod) } }

    val keyedRDD: RDD[(SpatialKey, MultibandTile)] =
      layoutsRDD.flatMap { layouts: Seq[LayoutTileSource] =>
        layouts.flatMap { layout: LayoutTileSource =>
          layout.keys.flatMap { key: SpatialKey =>
            layout.rasterRef(key).raster match {
              case Some(raster) => Some((key, raster.tile))
              case None => None
            }
          }
        }
      }

    /*
    val tiledRDD: RDD[(SpatialKey, MultibandTile)] =
      layoutRDD.flatMap { case source =>
        source.keys.toIterator.flatMap { key: SpatialKey =>
          source.rasterRef(key).raster match {
            case Some(raster) => Some((key, raster.tile))
            case None => None
          }
        }
      }

    rasterSourceRDD.unpersist()

    val contextRDD: MultibandTileLayerRDD[SpatialKey] =
      ContextRDD(tiledRDD, tileLayerMetadata)

    SpatialTiledRasterLayer(zoom, contextRDD)
    */

    ???
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
