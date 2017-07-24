package geopyspark.geotrellis

import geopyspark.geotrellis.GeoTrellisUtils._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.wkt.WKT
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.raster.render._
import geotrellis.raster.histogram.Histogram
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.compression._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.tiling._

import spray.json._
import spray.json.DefaultJsonProtocol._

import spire.syntax.order._
import spire.std.any._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.storage.StorageLevel

import scala.reflect._
import scala.util._
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap


/**
 * RDD of Rasters, untiled and unsorted
 */
abstract class RasterRDD[K](implicit ev0: ClassTag[K], ev1: Component[K, ProjectedExtent]) extends TileRDD[K] {
  def rdd: RDD[(K, MultibandTile)]

  def toProtoRDD(): JavaRDD[Array[Byte]]

  def bands(band: Int): RasterRDD[K] =
    withRDD(rdd.mapValues { multibandTile => multibandTile.subsetBands(band) })

  def bands(bands: java.util.ArrayList[Int]): RasterRDD[K] =
    withRDD(rdd.mapValues { multibandTile => multibandTile.subsetBands(bands.asScala) })

  def collectMetadata(
    layoutDefinition: LayoutDefinition,
    crs: String
  ): String = {
    collectMetadata(Right(layoutDefinition), TileRDD.getCRS(crs))
  }

  def collectMetadata(tileSize: String, crs: String): String = {
    val layoutScheme =
      if (tileSize != "")
        Left(FloatingLayoutScheme(tileSize.toInt))
      else
        Left(FloatingLayoutScheme())

    collectMetadata(layoutScheme, TileRDD.getCRS(crs))
  }

  def convertDataType(newType: String): RasterRDD[_] =
    withRDD(rdd.map { x => (x._1, x._2.convert(CellType.fromName(newType))) })

  protected def collectMetadata(layout: Either[LayoutScheme, LayoutDefinition], crs: Option[CRS]): String

  protected def tileToLayout(tileLayerMetadata: String, resampleMethod: ResampleMethod): TiledRasterRDD[_]
  def tileToLayout(layoutType: LayoutType, resampleMethod: ResampleMethod): TiledRasterRDD[_]
  protected def tileToLayout(layoutDefinition: LayoutDefinition, resampleMethod: ResampleMethod): TiledRasterRDD[_]

  protected def reproject(targetCRS: String, resampleMethod: ResampleMethod): RasterRDD[K]
  protected def reproject(targetCRS: String, layoutType: LayoutType, resampleMethod: ResampleMethod): TiledRasterRDD[_]
  protected def reproject(targetCRS: String, layoutDefinition: LayoutDefinition, resampleMethod: ResampleMethod): TiledRasterRDD[_]
  protected def withRDD(result: RDD[(K, MultibandTile)]): RasterRDD[K]
}
