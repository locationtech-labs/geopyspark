package geopyspark.geotrellis.vlm

import geopyspark.geotrellis.{PartitionStrategy, SpatialTiledRasterLayer}
import geopyspark.geotrellis.Constants.{GEOTRELLIS, GDAL}

import geotrellis.contrib.vlm._
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


object RasterSource {
  def readToLayout(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[String],
    layoutType: LayoutType,
    targetCRS: Option[String],
    resampleMethod: ResampleMethod,
    partitionStrategy: Option[PartitionStrategy],
    readMethod: String
  ): SpatialTiledRasterLayer =
    readToLayout(
      sc,
      layerType,
      sc.parallelize(paths.asScala, paths.size),
      layoutType,
      targetCRS,
      resampleMethod,
      partitionStrategy,
      readMethod
    )

  def readToLayout(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[String],
    layoutType: LayoutType,
    targetCRS: Option[String],
    resampleMethod: ResampleMethod,
    partitionStrategy: Option[PartitionStrategy],
    readMethod: String
  ): SpatialTiledRasterLayer = {
    val rasterSourceRDD: RDD[RasterSource] =
      (readMethod match {
        case GEOTRELLIS => rdd.map { new GeoTiffRasterSource(_): RasterSource }
        case GDAL => rdd.map { GDALRasterSource(_): RasterSource }
      }).cache()

    val reprojectedSourcesRDD: RDD[RasterSource] =
      targetCRS match {
        case Some(crs) =>
          rasterSourceRDD.map { _.reproject(CRS.fromString(crs), resampleMethod) }.cache()
        case None =>
          rasterSourceRDD
      }

    val metadata: RasterSummary = RasterSummary.fromRDD(reprojectedSourcesRDD)

    val layoutScheme: LayoutScheme =
      layoutType match {
        case global: GlobalLayout => ZoomedLayoutScheme(metadata.crs, global.tileSize)
        case local: LocalLayout => FloatingLayoutScheme(local.tileCols, local.tileRows)
      }

    val layout: LayoutDefinition = metadata.levelFor(layoutScheme).layout
    val layoutRDD: RDD[LayoutTileSource] = reprojectedSourcesRDD.map { _.tileToLayout(layout, resampleMethod) }

    val collectedMetadata: RasterSummary = RasterSummary.fromRDD(layoutRDD.map { _.source })

    val (tileLayerMetadata, zoom): (TileLayerMetadata[SpatialKey], Option[Int]) =
      collectedMetadata.toTileLayerMetadata(layoutType)

    val tiledRDD: RDD[(SpatialKey, MultibandTile)] =
      layoutRDD.flatMap { case source =>
        source.keys.toIterator.flatMap { key: SpatialKey =>
          source.rasterRef(key).raster match {
            case Some(raster) => Some((key, raster.tile))
            case None => None
          }
        }
      }

    reprojectedSourcesRDD.unpersist()
    rasterSourceRDD.unpersist()

    val contextRDD: MultibandTileLayerRDD[SpatialKey] =
      ContextRDD(tiledRDD, tileLayerMetadata)

    SpatialTiledRasterLayer(zoom, contextRDD)
  }
}
