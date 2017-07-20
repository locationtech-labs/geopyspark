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
    extent: java.util.Map[String, Double],
    layout: java.util.Map[String, Int],
    crs: String
  ): String = {
    val layoutDefinition = Right(LayoutDefinition(extent.toExtent, layout.toTileLayout))

    collectMetadata(layoutDefinition, TileRDD.getCRS(crs))
  }

  def collectMetadata(tileSize: String, crs: String): String = {
    val layoutScheme =
      if (tileSize != "")
        Left(FloatingLayoutScheme(tileSize.toInt))
      else
        Left(FloatingLayoutScheme())

    collectMetadata(layoutScheme, TileRDD.getCRS(crs))
  }

  /** Collect [[RasterSummary]] from unstructred rasters, grouped by CRS */
  def collectRasterSummary[
    K2: SpatialComponent: Boundable
  ](implicit ev: (K => TilerKeyMethods[K, K2])): Seq[RasterSummary[K2]] = {

    rdd
      .map { case (key, grid) =>
        val ProjectedExtent(extent, crs) = key.getComponent[ProjectedExtent]
        // Bounds are return to set the non-spatial dimensions of the KeyBounds;
        // the spatial KeyBounds are set outside this call.
        val boundsKey = key.translate(SpatialKey(0,0))
        val cellSize = CellSize(extent, grid.cols, grid.rows)
        HashMap(crs -> RasterSummary(crs, grid.cellType, cellSize, extent, KeyBounds(boundsKey, boundsKey), 1))
      }
      .reduce { (m1, m2) => m1.merged(m2){ case ((k,v1), (_,v2)) => (k,v1 combine v2) } }
      .values.toSeq
  }

  def tileToLayout(
    layoutType: LayoutType,
    rm: ResampleMethod,
    force_crs: String,
    force_cellType: String
  ): TiledRasterRDD[_]

  def convertDataType(newType: String): RasterRDD[_] =
    withRDD(rdd.map { x => (x._1, x._2.convert(CellType.fromName(newType))) })

  protected def collectMetadata(layout: Either[LayoutScheme, LayoutDefinition], crs: Option[CRS]): String
  protected def cutTiles(layerMetadata: String, resampleMethod: String): TiledRasterRDD[_]
  protected def tileToLayout(tileLayerMetadata: String, resampleMethod: String): TiledRasterRDD[_]
  protected def reproject(target_crs: String, resampleMethod: String): RasterRDD[_]
  protected def withRDD(result: RDD[(K, MultibandTile)]): RasterRDD[K]
}
