package geopyspark.geotrellis.vlm

import geopyspark.geotrellis.{PartitionStrategy, TiledRasterLayer}

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
  def tileToLayout(
    sc: SparkContext,
    layerType: String,
    paths: java.util.ArrayList[String],
    layout: LayoutScheme,
    targetCRS: Option[String],
    resampleMethod: ResampleMethod,
    partitionStrategy: Option[PartitionStrategy],
    readMethod: String
  ): TiledRasterLayer[_] =
    tileToLayout(
      sc,
      layerType,
      sc.parallelize(paths.asScala, paths.size),
      layout,
      targetCRS,
      resampleMethod,
      partitionStrategy,
      readMethod
    )

  def tileToLayout(
    sc: SparkContext,
    layerType: String,
    rdd: RDD[String],
    layout: LayoutScheme,
    targetCRS: Option[String],
    resampleMethod: ResampleMethod,
    partitionStrategy: Option[PartitionStrategy],
    readMethod: String
  ): TiledRasterLayer[_] = {
    val rasterSourceRDD: RDD[RasterSource] =
      readMethod match {
        case "GeoTrellis" => rdd.map { new GeoTiffRasterSource(_) }
        case "GDAL" => rdd.map { GDALRasterSource }
      }

    ???
  }
}
