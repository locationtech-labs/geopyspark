package geopyspark.geotrellis

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.tiling.{LayoutDefinition, LayoutScheme}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import protos.tupleMessages.ProtoTuple
import spray.json._

import scala.util.{Either, Left, Right}

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

  def cutTiles(layerMetadata: String, resampleMethod: ResampleMethod): TiledRasterRDD[SpaceTimeKey] = {
    val md = layerMetadata.parseJson.convertTo[TileLayerMetadata[SpaceTimeKey]]
    val tiles = rdd.cutTiles[SpaceTimeKey](md, resampleMethod)
    new TemporalTiledRasterRDD(None, MultibandTileLayerRDD(tiles, md))
  }

  def tileToLayout(layerMetadata: String, resampleMethod: ResampleMethod): TiledRasterRDD[SpaceTimeKey] = {
    val md = layerMetadata.parseJson.convertTo[TileLayerMetadata[SpaceTimeKey]]
    new TemporalTiledRasterRDD(None, MultibandTileLayerRDD(rdd.tileToLayout(md, resampleMethod), md))
  }

  def tileToLayout(layoutType: LayoutType, resampleMethod: ResampleMethod): TiledRasterRDD[SpaceTimeKey] ={
    val sms = RasterSummary.collect[TemporalProjectedExtent, SpaceTimeKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head
    val (metadata, zoom) = sm.toTileLayerMetadata(layoutType)
    val tiled = rdd.tileToLayout(metadata, resampleMethod)
    new TemporalTiledRasterRDD(zoom, MultibandTileLayerRDD(tiled, metadata))
  }

  def reproject(targetCRS: String, resampleMethod: ResampleMethod): TemporalRasterRDD = {
    val crs = TileRDD.getCRS(targetCRS).get
    new TemporalRasterRDD(rdd.reproject(crs, resampleMethod))
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

object TemporalRasterRDD {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): TemporalRasterRDD =
    TemporalRasterRDD(
      PythonTranslator.fromPython[
        (TemporalProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(TemporalProjectedExtent, MultibandTile)]): TemporalRasterRDD =
    new TemporalRasterRDD(rdd)
}
