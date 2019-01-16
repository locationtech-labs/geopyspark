package geopyspark.geotrellis.vlm

import geopyspark.geotrellis.{PartitionStrategy, ProjectedRasterLayer, SpatialTiledRasterLayer, SpatialPartitioner}
import geopyspark.geotrellis.{LayoutType => GPSLayoutType, LocalLayout => GPSLocalLayout, GlobalLayout => GPSGlobalLayout}

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
    numPartitions: Integer,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): ProjectedRasterLayer = {
    val partitions =
      numPartitions match {
        case i: Integer => Some(i.toInt)
        case null => None
      }

    read(
      sc,
      layerType,
      sc.parallelize(paths.asScala, partitions.getOrElse(sc.defaultParallelism)),
      targetCRS,
      resampleMethod,
      readMethod
    )
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
    paths: java.util.ArrayList[java.util.HashMap[String, String]],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): ProjectedRasterLayer = {
    val scalaPaths: Seq[Seq[(String, String)]] = paths.asScala.toSeq.map { _.asScala.toSeq }

    readOrdered(
      sc,
      layerType,
      sc.parallelize(scalaPaths, scalaPaths.size),
      targetCRS,
      resampleMethod,
      readMethod
    )
  }

  def readOrdered(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[Seq[(String, String)]],
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): ProjectedRasterLayer = {
    val rasterSourcesRDD: RDD[Seq[(String, RasterSource)]] =
      (readMethod match {
        case GEOTRELLIS =>
          rdd.mapPartitions { iter =>
            iter.map { files: Seq[(String, String)] =>
              files.map { case (k, v) => (k, new GeoTiffRasterSource(v): RasterSource)
              }
            }
          }
        case GDAL =>
          rdd.mapPartitions { iter =>
            iter.map { files: Seq[(String, String)] =>
              files.map { case (k, v) => (k, GDALRasterSource(v): RasterSource)
              }
            }
          }
      }).cache()

    val reprojectedSourcesRDD: RDD[(String, RasterSource)] =
      targetCRS match {
        case crs: String =>
          rasterSourcesRDD.flatMap { case sources =>
            sources.map { case (index, source) =>
              (index, source.reproject(CRS.fromString(crs), resampleMethod))
            }
          }
        case null => rasterSourcesRDD.flatMap { sources => sources }
      }

    val rasterSummary: RasterSummary =
      reprojectedSourcesRDD
        .map { case (_, source) =>
            val ProjectedExtent(extent, crs) = source.getComponent[ProjectedExtent]
            val cellSize = CellSize(extent, source.cols, source.rows)
            RasterSummary(crs, source.cellType, cellSize, extent, source.size, 1)
          }.reduce { _ combine _ }

    val keyedSourcesRDD: RDD[(Extent, (String, RasterSource))] =
      reprojectedSourcesRDD.map { case (index, source) =>
        (source.extent, (index, source))
      }

    val groupedSourcesRDD: RDD[(Extent, Iterable[(String, RasterSource)])] =
      keyedSourcesRDD.groupByKey(rasterSummary.estimatePartitionsNumber)

    val projectedRDD: RDD[(ProjectedExtent, MultibandTile)] =
      groupedSourcesRDD.map { case (ex, iter) =>
        val sorted = iter.toSeq.sortBy { _._1 }
        val crs = sorted.head._2.crs

        val tiles: Seq[MultibandTile] =
          sorted.flatMap { case (_, source) =>
            source.read(source.extent) match {
              case Some(raster) => Some(raster.tile)
              case None => None
            }
          }

        (ProjectedExtent(ex, crs), MultibandTile(tiles.map { _.band(0) }))
      }

    rasterSourcesRDD.unpersist()

    ProjectedRasterLayer(projectedRDD)
  }

  def readToLayout(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[String],
    layoutType: GPSLayoutType,
    targetCRS: String,
    numPartitions: Integer,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): SpatialTiledRasterLayer = {
    val partitions =
      numPartitions match {
        case i: Integer => Some(i.toInt)
        case null => None
      }

    readToLayout(
      sc,
      layerType,
      sc.parallelize(paths.asScala, partitions.getOrElse(sc.defaultParallelism)),
      layoutType,
      targetCRS,
      resampleMethod,
      readMethod
    )
  }

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

    SpatialTiledRasterLayer(zoom, contextRDD)
  }

  def readOrderedToLayout(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[java.util.HashMap[String, String]],
    layoutType: GPSLayoutType,
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): SpatialTiledRasterLayer = {
    val scalaPaths: Seq[Seq[(String, String)]] = paths.asScala.toSeq.map { _.asScala.toSeq }

    readOrderedToLayout(
      sc,
      layerType,
      sc.parallelize(scalaPaths, scalaPaths.size),
      layoutType,
      targetCRS,
      resampleMethod,
      readMethod
    )
  }

  def readOrderedToLayout(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[Seq[(String, String)]],
    layoutType: LayoutType,
    targetCRS: String,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): SpatialTiledRasterLayer = {
    val rasterSourcesRDD: RDD[Seq[(String, RasterSource)]] =
      (readMethod match {
        case GEOTRELLIS =>
          rdd.mapPartitions { iter =>
            iter.map { files: Seq[(String, String)] =>
              files.map { case (k, v) => (k, new GeoTiffRasterSource(v): RasterSource)
              }
            }
          }
        case GDAL =>
          rdd.mapPartitions { iter =>
            iter.map { files: Seq[(String, String)] =>
              files.map { case (k, v) => (k, GDALRasterSource(v): RasterSource)
              }
            }
          }
      }).cache()

    val reprojectedSourcesRDD: RDD[(String, RasterSource)] =
      targetCRS match {
        case crs: String =>
          rasterSourcesRDD.flatMap { case sources =>
            sources.map { case (index, source) =>
              (index, source.reproject(CRS.fromString(crs), resampleMethod))
            }
          }
        case null => rasterSourcesRDD.flatMap { sources => sources }
      }

    val rasterSummary: RasterSummary =
      reprojectedSourcesRDD
        .map { case (_, source) =>
            val ProjectedExtent(extent, crs) = source.getComponent[ProjectedExtent]
            val cellSize = CellSize(extent, source.cols, source.rows)
            RasterSummary(crs, source.cellType, cellSize, extent, source.size, 1)
          }.reduce { _ combine _ }

    val LayoutLevel(zoom, layout) =
      layoutType match {
        case global: GlobalLayout =>
          val scheme = ZoomedLayoutScheme(rasterSummary.crs, global.tileSize)
          scheme.levelForZoom(global.zoom)
        case local: LocalLayout =>
          val scheme = FloatingLayoutScheme(local.tileCols, local.tileRows)
          rasterSummary.levelFor(scheme)
      }

    val tileLayerMetadata: TileLayerMetadata[SpatialKey] =
      rasterSummary.toTileLayerMetadata(layout, zoom)._1

    val layoutsRDD: RDD[(String, LayoutTileSource)] =
      reprojectedSourcesRDD.mapValues { _.tileToLayout(layout, resampleMethod) }

    val rasterRegionRDD: RDD[(SpatialKey, (String, RasterRegion))] =
      layoutsRDD.flatMap { case (index, source) =>
        source.keyedRasterRegions().map { case (key, region) =>
          (key, (index, region))
        }
      }

    val groupedRDD: RDD[(SpatialKey, Iterable[(String, RasterRegion)])] =
      rasterRegionRDD.groupByKey(SpatialPartitioner(rasterSummary.estimatePartitionsNumber))

    val tiledRDD: RDD[(SpatialKey, MultibandTile)] =
      groupedRDD.mapValues { iter =>
        val sorted = iter.toSeq.sortBy { _._1 }

        MultibandTile(sorted.flatMap { case (_, ref) => ref.raster.toSeq.flatMap { _.tile.bands } })
      }

    rasterSourcesRDD.unpersist()

    val contextRDD: MultibandTileLayerRDD[SpatialKey] =
      ContextRDD(tiledRDD, tileLayerMetadata)

    SpatialTiledRasterLayer(zoom, contextRDD)
  }

  implicit def gps2VLM(layoutType: GPSLayoutType): LayoutType =
    layoutType match {
      case local: GPSLocalLayout => LocalLayout(local.tileCols, local.tileRows)
      case global: GPSGlobalLayout => GlobalLayout(global.tileSize, global.zoom, global.threshold)
    }
}
