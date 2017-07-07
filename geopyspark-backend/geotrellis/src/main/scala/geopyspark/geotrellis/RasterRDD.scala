package geopyspark.geotrellis

import geopyspark.geotrellis.GeoTrellisUtils._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.vector._
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

import java.util.Map

import protos.tupleMessages._
import protos.extentMessages._


object TileRDD {
  import Constants._

  def getResampleMethod(resampleMethod: String): ResampleMethod =
    resampleMethod match {
      case NEARESTNEIGHBOR => NearestNeighbor
      case BILINEAR => Bilinear
      case CUBICCONVOLUTION => CubicConvolution
      case CUBICSPLINE => CubicSpline
      case LANCZOS => Lanczos
      case AVERAGE => Average
      case MODE => Mode
      case MEDIAN => Median
      case MAX => Max
      case MIN => Min
    }

  def getCRS(crs: String): Option[CRS] = {
    Try(CRS.fromName(crs))
      .recover({ case e => CRS.fromString(crs) })
      .recover({ case e => CRS.fromEpsgCode(crs.toInt) })
      .toOption
  }

  def getStorageMethod(
    storageMethod: String,
    rowsPerStrip: Int,
    tileDimensions: java.util.ArrayList[Int]
  ): StorageMethod =
    (storageMethod, rowsPerStrip) match {
      case (STRIPED, 0) => Striped()
      case (STRIPED, x) => Striped(x)
      case (TILED, _) => Tiled(tileDimensions.get(0), tileDimensions.get(1))
    }

  def getCompression(compressionType: String): Compression =
    compressionType match {
      case NOCOMPRESSION => NoCompression
      case DEFLATECOMPRESSION => DeflateCompression
    }
}

abstract class TileRDD[K: ClassTag] {
  def rdd: RDD[(K, MultibandTile)]
  def keyClass: Class[_] = classTag[K].runtimeClass
  def keyClassName: String = keyClass.getName

  def toPngRDD(cm: ColorMap): JavaRDD[Array[Byte]] =
    toPngRDD(rdd.mapValues { v => v.bands(0).renderPng(cm).bytes })

  def toPngRDD(pngRDD: RDD[(K, Array[Byte])]): JavaRDD[Array[Byte]]

  def reclassify(
    intMap: java.util.Map[Int, Int],
    boundaryType: String,
    replaceNoDataWith: Int
  ): TileRDD[_] = {
    val scalaMap = intMap.asScala.toMap

    val boundary = getBoundary(boundaryType)
    val mapStrategy = new MapStrategy(boundary, replaceNoDataWith, NODATA, false)
    val breakMap = new BreakMap(scalaMap, mapStrategy, { i: Int => isNoData(i) })

    val reclassifiedRDD =
      rdd.mapValues { x =>
        val count = x.bandCount
        val tiles = Array.ofDim[Tile](count)

        for (y <- 0 until count) {
          val band = x.band(y)
          tiles(y) = band.map(i => breakMap.apply(i))
        }

        MultibandTile(tiles)
      }
    reclassify(reclassifiedRDD)
  }

  def persist(newLevel: StorageLevel): Unit = {
    // persist call changes the state of the SparkContext rather than RDD object
    rdd.persist(newLevel)
  }

  def unpersist(): Unit = {
    rdd.unpersist()
  }

  def reclassifyDouble(
    doubleMap: java.util.Map[Double, Double],
    boundaryType: String,
    replaceNoDataWith: Double
  ): TileRDD[_] = {
    val scalaMap = doubleMap.asScala.toMap

    val boundary = getBoundary(boundaryType)
    val mapStrategy = new MapStrategy(boundary, replaceNoDataWith, doubleNODATA, false)
    val breakMap = new BreakMap(scalaMap, mapStrategy, { d: Double => isNoData(d) })

    val reclassifiedRDD =
      rdd.mapValues { x =>
        val count = x.bandCount
        val tiles = Array.ofDim[Tile](count)

        for (y <- 0 until count) {
          val band = x.band(y)
          tiles(y) = band.mapDouble(i => breakMap.apply(i))
        }

        MultibandTile(tiles)
      }
    reclassifyDouble(reclassifiedRDD)
  }

  def getMinMax: (Double, Double) = {
    val minMaxs: Array[(Double, Double)] = rdd.histogram.map{ x => x.minMaxValues.get }

    minMaxs.foldLeft(minMaxs(0)) {
      (acc, elem) =>
        (math.min(acc._1, elem._1), math.max(acc._2, elem._2))
    }
  }

  /** Compute the quantile breaks per band.
    * TODO: This just works for single bands right now.
    *       make it work with multiband.
    */
  def quantileBreaks(n: Int): Array[Double] =
    rdd
      .histogram
      .head
      .quantileBreaks(n)

  /** Compute the quantile breaks per band.
    * TODO: This just works for single bands right now.
    *       make it work with multiband.
    */
  def quantileBreaksExactInt(n: Int): Array[Int] =
    rdd
      .mapValues(_.band(0))
      .histogramExactInt
      .quantileBreaks(n)

  protected def reclassify(reclassifiedRDD: RDD[(K, MultibandTile)]): TileRDD[_]
  protected def reclassifyDouble(reclassifiedRDD: RDD[(K, MultibandTile)]): TileRDD[_]
}


/**
 * RDD of Rasters, untiled and unsorted
 */
abstract class RasterRDD[K: ClassTag] extends TileRDD[K] {
  def rdd: RDD[(K, MultibandTile)]

  def toProtoRDD(): JavaRDD[Array[Byte]]

  def bands(band: Int): RasterRDD[K] =
    withRDD(rdd.mapValues { multibandTile => multibandTile.subsetBands(band) })

  def bands(bands: java.util.ArrayList[Int]): RasterRDD[K] =
    withRDD(rdd.mapValues { multibandTile => multibandTile.subsetBands(bands.asScala) })

  def toGeoTiffRDD(
    storageMethod: String,
    rowsPerStrip: Int,
    tileDimensions: java.util.ArrayList[Int],
    compression: String,
    colorSpace: Int,
    headTags: java.util.Map[String, String],
    bandTags: java.util.ArrayList[java.util.Map[String, String]]
  ): JavaRDD[Array[Byte]] = {
    val storage = TileRDD.getStorageMethod(storageMethod, rowsPerStrip, tileDimensions)
    val tags =
      if (headTags.isEmpty || bandTags.isEmpty)
        Tags.empty
      else
        Tags(headTags.asScala.toMap,
          bandTags.toArray.map(_.asInstanceOf[scala.collection.immutable.Map[String, String]]).toList)

    val options = GeoTiffOptions(storage,
      TileRDD.getCompression(compression),
      colorSpace,
      None)

    toGeoTiffRDD(tags, options)
  }

  def toGeoTiffRDD(
    tags: Tags,
    geotiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]]

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

  def convertDataType(newType: String): RasterRDD[_] =
    withRDD(rdd.map { x => (x._1, x._2.convert(CellType.fromName(newType))) })

  protected def collectMetadata(layout: Either[LayoutScheme, LayoutDefinition], crs: Option[CRS]): String
  protected def cutTiles(layerMetadata: String, resampleMethod: String): TiledRasterRDD[_]
  protected def tileToLayout(tileLayerMetadata: String, resampleMethod: String): TiledRasterRDD[_]
  protected def reproject(target_crs: String, resampleMethod: String): RasterRDD[_]
  protected def withRDD(result: RDD[(K, MultibandTile)]): RasterRDD[K]
}

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

  def cutTiles(layerMetadata: String, resampleMethod: String): TiledRasterRDD[SpatialKey] = {
    val md = layerMetadata.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    val rm = TileRDD.getResampleMethod(resampleMethod)
    new SpatialTiledRasterRDD(None, MultibandTileLayerRDD(rdd.cutTiles(md, rm), md))
  }

  def tileToLayout(tileLayerMetadata: String, resampleMethod: String): TiledRasterRDD[SpatialKey] = {
    val md = tileLayerMetadata.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    val rm = TileRDD.getResampleMethod(resampleMethod)
    new SpatialTiledRasterRDD(None, MultibandTileLayerRDD(rdd.tileToLayout(md, rm), md))
  }

  def reproject(targetCRS: String, resampleMethod: String): ProjectedRasterRDD = {
    val crs = TileRDD.getCRS(targetCRS).get
    val resample = TileRDD.getResampleMethod(resampleMethod)
    new ProjectedRasterRDD(rdd.reproject(crs, resample))
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


class TemporalRasterRDD(val rdd: RDD[(TemporalProjectedExtent, MultibandTile)]) extends RasterRDD[TemporalProjectedExtent] {

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

  def cutTiles(layerMetadata: String, resampleMethod: String): TiledRasterRDD[SpaceTimeKey] = {
    val md = layerMetadata.parseJson.convertTo[TileLayerMetadata[SpaceTimeKey]]
    val rm = TileRDD.getResampleMethod(resampleMethod)
    val tiles = rdd.cutTiles[SpaceTimeKey](md, rm)
    new TemporalTiledRasterRDD(None, MultibandTileLayerRDD(tiles, md))
  }

  def tileToLayout(layerMetadata: String, resampleMethod: String): TiledRasterRDD[SpaceTimeKey] = {
    val md = layerMetadata.parseJson.convertTo[TileLayerMetadata[SpaceTimeKey]]
    val rm = TileRDD.getResampleMethod(resampleMethod)
    new TemporalTiledRasterRDD(None, MultibandTileLayerRDD(rdd.tileToLayout(md, rm), md))
  }

  def reproject(targetCRS: String, resampleMethod: String): TemporalRasterRDD = {
    val crs = TileRDD.getCRS(targetCRS).get
    val resample = TileRDD.getResampleMethod(resampleMethod)
    new TemporalRasterRDD(rdd.reproject(crs, resample))
  }

  def reclassify(reclassifiedRDD: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterRDD[TemporalProjectedExtent] =
    TemporalRasterRDD(reclassifiedRDD)

  def reclassifyDouble(reclassifiedRDD: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterRDD[TemporalProjectedExtent] =
    TemporalRasterRDD(reclassifiedRDD)

  def withRDD(result: RDD[(TemporalProjectedExtent, MultibandTile)]): RasterRDD[TemporalProjectedExtent] =
    TemporalRasterRDD(result)

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


object ProjectedRasterRDD {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): ProjectedRasterRDD =
    ProjectedRasterRDD(
      PythonTranslator.fromPython[
        (ProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(ProjectedExtent, MultibandTile)]): ProjectedRasterRDD =
    new ProjectedRasterRDD(rdd)
}

object TemporalRasterRDD {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): TemporalRasterRDD =
    TemporalRasterRDD(
      PythonTranslator.fromPython[
        (TemporalProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(TemporalProjectedExtent, MultibandTile)]): TemporalRasterRDD =
    new TemporalRasterRDD(rdd)
}
