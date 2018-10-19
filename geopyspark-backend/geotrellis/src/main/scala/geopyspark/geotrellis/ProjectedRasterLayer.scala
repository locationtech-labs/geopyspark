package geopyspark.geotrellis

import geopyspark.util._
import GeoTrellisUtils._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.{GeoTiffOptions, MultibandGeoTiff, Tags}
import geotrellis.raster.resample.ResampleMethod
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.vector._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark._
import org.apache.spark.rdd._

import protos.tupleMessages.ProtoTuple
import protos.extentMessages.ProtoProjectedExtent

import scala.util.{Either, Left, Right}
import scala.collection.JavaConverters._

import java.util.ArrayList

import spray.json._


class ProjectedRasterLayer(val rdd: RDD[(ProjectedExtent, MultibandTile)]) extends RasterLayer[ProjectedExtent] {

  def collectMetadata(layoutType: LayoutType): String = {
    val sms = RasterSummary.collect[ProjectedExtent, SpatialKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")

    sms.head.toTileLayerMetadata(layoutType)._1.toJson.compactPrint
  }

  def collectMetadata(layoutDefinition: LayoutDefinition): String = {
    val sms = RasterSummary.collect[ProjectedExtent, SpatialKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")
    val sm = sms.head

    TileLayerMetadata[SpatialKey](
      sm.cellType,
      layoutDefinition,
      sm.extent,
      sm.crs,
      sm.bounds.setSpatialBounds(layoutDefinition.mapTransform(sm.extent))
    ).toJson.compactPrint
  }

  def tileToLayout(
    tileLayerMetadata: String,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpatialKey] = {
    val md = tileLayerMetadata.parseJson.convertTo[TileLayerMetadata[SpatialKey]]
    val options = getTilerOptions(resampleMethod, partitionStrategy)

    new SpatialTiledRasterLayer(None, MultibandTileLayerRDD(rdd.tileToLayout(md, options), md))
  }

  def tileToLayout(
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpatialKey] = {
    val sms = RasterSummary.collect[ProjectedExtent, SpatialKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")

    val sm = sms.head
    val metadata = TileLayerMetadata[SpatialKey](
      sm.cellType,
      layoutDefinition,
      sm.extent,
      sm.crs,
      sm.bounds.setSpatialBounds(layoutDefinition.mapTransform(sm.extent))
    )

    val options = getTilerOptions(resampleMethod, partitionStrategy)

    SpatialTiledRasterLayer(None, MultibandTileLayerRDD(rdd.tileToLayout(metadata, options), metadata))
  }

  def tileToLayout(
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpatialKey] = {
    val sms = RasterSummary.collect[ProjectedExtent, SpatialKey](rdd)
    require(sms.length == 1, s"Multiple raster CRS layers found: ${sms.map(_.crs).toList}")

    val sm = sms.head
    val (metadata, zoom) = sm.toTileLayerMetadata(layoutType)
    val options = getTilerOptions(resampleMethod, partitionStrategy)
    val tiled = rdd.tileToLayout(metadata, options)

    new SpatialTiledRasterLayer(zoom, MultibandTileLayerRDD(tiled, metadata))
  }

  def reproject(targetCRS: String, resampleMethod: ResampleMethod): ProjectedRasterLayer = {
    val crs = TileLayer.getCRS(targetCRS).get
    new ProjectedRasterLayer(rdd.reproject(crs, resampleMethod))
  }

  def reproject(
    targetCRS: String,
    layoutType: LayoutType,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpatialKey] = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)

    val crs = TileLayer.getCRS(targetCRS).get
    val tiled = tileToLayout(LocalLayout(256), resampleMethod, partitionStrategy).rdd

    layoutType match {
      case GlobalLayout(tileSize, null, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (zoom, reprojected) = tiled.reproject(crs, scheme, resampleMethod, partitioner)
        new SpatialTiledRasterLayer(Some(zoom), reprojected)

      case GlobalLayout(tileSize, zoom, threshold) =>
        val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
        val (_, reprojected) = TileRDDReproject(tiled, crs, Right(scheme.levelForZoom(zoom).layout), resampleMethod, partitioner)
        new SpatialTiledRasterLayer(Some(zoom), reprojected)

      case LocalLayout(tileCols, tileRows) =>
        val (_, reprojected) = tiled.reproject(crs, FloatingLayoutScheme(tileCols, tileRows), resampleMethod, partitioner)
        new SpatialTiledRasterLayer(None, reprojected)
    }
  }

  def reproject(
    target_crs: String,
    layoutDefinition: LayoutDefinition,
    resampleMethod: ResampleMethod,
    partitionStrategy: PartitionStrategy
  ): TiledRasterLayer[SpatialKey] = {
    val partitioner = TileLayer.getPartitioner(partitionStrategy, rdd.getNumPartitions)

    val tiled = tileToLayout(layoutDefinition, resampleMethod, partitionStrategy).rdd
    val (zoom, reprojected) =
      TileRDDReproject(tiled, TileLayer.getCRS(target_crs).get, Right(layoutDefinition), resampleMethod, partitioner)

    SpatialTiledRasterLayer(Some(zoom), reprojected)
  }

  def reclassify(reclassifiedRDD: RDD[(ProjectedExtent, MultibandTile)]): RasterLayer[ProjectedExtent] =
    ProjectedRasterLayer(reclassifiedRDD)

  def reclassifyDouble(reclassifiedRDD: RDD[(ProjectedExtent, MultibandTile)]): RasterLayer[ProjectedExtent] =
    ProjectedRasterLayer(reclassifiedRDD)

  def withRDD(result: RDD[(ProjectedExtent, MultibandTile)]): RasterLayer[ProjectedExtent] =
    ProjectedRasterLayer(result)

  def toProtoRDD(): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(ProjectedExtent, MultibandTile), ProtoTuple](rdd)

  def toPngRDD(pngRDD: RDD[(ProjectedExtent, Array[Byte])]): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[(ProjectedExtent, Array[Byte]), ProtoTuple](pngRDD)

  def toGeoTiffRDD(
    tags: Tags,
    resampleMethod: ResampleMethod,
    decimations: List[Int],
    geoTiffOptions: GeoTiffOptions
  ): JavaRDD[Array[Byte]] = {
    val geotiffRDD =
      rdd.map { case (k, v) =>
        val geoTiff =
          MultibandGeoTiff(v, k.extent, k.crs, tags, geoTiffOptions)
            .withOverviews(resampleMethod, decimations)

        (k, geoTiff.toByteArray)
      }

    PythonTranslator.toPython[(ProjectedExtent, Array[Byte]), ProtoTuple](geotiffRDD)
  }

  def collectKeys(): java.util.ArrayList[Array[Byte]] =
    PythonTranslator.toPython[ProjectedExtent, ProtoProjectedExtent](rdd.keys.collect)
}


object ProjectedRasterLayer {
  def fromProtoEncodedRDD(javaRDD: JavaRDD[Array[Byte]]): ProjectedRasterLayer =
    ProjectedRasterLayer(
      PythonTranslator.fromPython[
        (ProjectedExtent, MultibandTile), ProtoTuple
      ](javaRDD, ProtoTuple.parseFrom))

  def apply(rdd: RDD[(ProjectedExtent, MultibandTile)]): ProjectedRasterLayer =
    new ProjectedRasterLayer(rdd)

  def unionLayers(
    sc: SparkContext,
    layers: ArrayList[ProjectedRasterLayer]
  ): ProjectedRasterLayer =
    ProjectedRasterLayer(sc.union(layers.asScala.map(_.rdd)))

  def combineBands(sc: SparkContext, layers: ArrayList[ProjectedRasterLayer]): ProjectedRasterLayer =
    ProjectedRasterLayer(TileLayer.combineBands[ProjectedExtent, ProjectedRasterLayer](sc, layers))
}
