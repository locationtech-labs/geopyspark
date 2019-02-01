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

import org.apache.spark.{SparkContext, Partitioner}
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
    val scalaPaths: Seq[String] = paths.asScala.toSeq

    val partitions =
      numPartitions match {
        case i: Integer => Some(i.toInt)
        case null => None
      }

    read(
      sc,
      layerType,
      sc.parallelize(scalaPaths, partitions.getOrElse(scalaPaths.size)),
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
    paths: java.util.ArrayList[String],
    maxTileSize: Int,
    targetCRS: String,
    numPartitions: Integer,
    resampleMethod: ResampleMethod,
    readMethod: String
  ): ProjectedRasterLayer = {
    val scalaPaths: Seq[Seq[(String, String)]] = ??? ///paths.asScala.toSeq.map { ReadingInfo( }

    val partitions =
      numPartitions match {
        case i: Integer => Some(i.toInt)
        case null => None
      }

    readOrdered(
      sc,
      layerType,
      sc.parallelize(scalaPaths, partitions.getOrElse(scalaPaths.size)),
      targetCRS,
      partitions,
      resampleMethod,
      readMethod
    )
  }

  def readOrdered(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[Seq[(String, String)]],
    targetCRS: String,
    numPartitions: Option[Int],
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
      keyedSourcesRDD.groupByKey(numPartitions.getOrElse(rasterSummary.estimatePartitionsNumber))

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
    val scalaPaths: Seq[String] = paths.asScala.toSeq

    val partitions =
      numPartitions match {
        case i: Integer => Some(i.toInt)
        case null => None
      }

    readToLayout(
      sc,
      layerType,
      sc.parallelize(scalaPaths, partitions.getOrElse(scalaPaths.size)),
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
    paths: java.util.ArrayList[SourceInfo],
    layoutType: GPSLayoutType,
    targetCRS: String,
    numPartitions: Integer,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy,
    readMethod: String
  ): SpatialTiledRasterLayer = {
    val scalaSources: Seq[SourceInfo] = paths.asScala.toSeq

    val partitions =
      numPartitions match {
        case i: Integer => Some(i.toInt)
        case null => None
      }

    val partitioner: Option[Partitioner] =
      partitionStrategy match {
        case ps: PartitionStrategy => ps.producePartitioner(partitions.getOrElse(scalaSources.size))
        case null => None
      }

    val crs: Option[CRS] =
      targetCRS match {
        case crs: String => Some(CRS.fromString(crs))
        case null => None
      }

    val transformSource: String => RasterSource =
      readMethod match {
        case GEOTRELLIS =>
          crs match {
            case Some(projection) =>
              (path: String) => GeoTiffRasterSource(path).reproject(projection, resampleMethod)
            case None =>
              (path: String) => GeoTiffRasterSource(path)
          }
        case GDAL =>
          crs match {
            case Some(projection) =>
              (path: String) => GDALRasterSource(path).reproject(projection, resampleMethod)
            case None =>
              (path: String) => GDALRasterSource(path)
          }
      }

    val sourceInfoRDD: RDD[SourceInfo] =
      sc.parallelize(scalaSources, partitions.getOrElse(scalaSources.size))

    val readingSourcesRDD: RDD[ReadingSource] =
      sourceInfoRDD.map { source =>
        val rasterSource = transformSource(source.source)

        ReadingSource(rasterSource, source.sourceToTargetBand)
      }

    val sourcesRDD: RDD[RasterSource] = readingSourcesRDD.map { _.source }

    val rasterSummary: RasterSummary = geotrellis.contrib.vlm.RasterSummary.fromRDD(sourcesRDD)

    val LayoutLevel(zoom, layout) =
      layoutType match {
        case global: GlobalLayout =>
          val scheme = ZoomedLayoutScheme(rasterSummary.crs, global.tileSize)
          scheme.levelForZoom(global.zoom)
        case local: LocalLayout =>
          val scheme = FloatingLayoutScheme(local.tileCols, local.tileRows)
          rasterSummary.levelFor(scheme)
      }

    val resampledSourcesRDD: RDD[ReadingSource] =
      readingSourcesRDD.map { source =>
        val resampledSource: RasterSource = source.source.resampleToGrid(layout, resampleMethod)

        source.copy(source = resampledSource)
      }

    val result = RasterSourceRDD.readFromRDD(resampledSourcesRDD, layout, partitioner)(sc)

    SpatialTiledRasterLayer(zoom, result)
  }

  implicit def gps2VLM(layoutType: GPSLayoutType): LayoutType =
    layoutType match {
      case local: GPSLocalLayout => LocalLayout(local.tileCols, local.tileRows)
      case global: GPSGlobalLayout => GlobalLayout(global.tileSize, global.zoom, global.threshold)
    }
}
