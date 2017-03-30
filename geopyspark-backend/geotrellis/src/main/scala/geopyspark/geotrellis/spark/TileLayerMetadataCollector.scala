package geopyspark.geotrellis.spark

import geopyspark.geotrellis._
import geopyspark.geotrellis.GeoTrellisUtils._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.json._
import geotrellis.spark.io.avro._
import geotrellis.spark.tiling._

import spray.json._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map

import scala.reflect.ClassTag


object TileLayerMetadataCollector {
  private def createCollection[
  K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
  K2: Boundable: SpatialComponent: JsonFormat
  ](
    returnedRdd: JavaRDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: java.util.Map[String, Double],
    pythonTileLayout: java.util.Map[String, Int],
    outputCrs: String
  ): String = {

    val rdd =
      PythonTranslator.fromPython[(K, MultibandTile)](returnedRdd, Some(schemaJson))

    val layoutDefinition = LayoutDefinition(pythonExtent.toExtent,
      pythonTileLayout.toTileLayout)

    val metadata =
      outputCrs match {
        case "" => rdd.collectMetadata[K2](layoutDefinition)
        case projection => rdd.collectMetadata[K2](CRS.fromName(projection), layoutDefinition)
      }

    metadata.toJson.compactPrint
  }

  def collectPythonMetadata(
    keyType: String,
    returnedRdd: JavaRDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: java.util.Map[String, Double],
    pythonTileLayout: java.util.Map[String, Int],
    outputCrs: String
  ): String =
    keyType match {
      case "ProjectedExtent" =>
        createCollection[ProjectedExtent, SpatialKey](
          returnedRdd,
          schemaJson,
          pythonExtent,
          pythonTileLayout,
          outputCrs)
      case "TemporalProjectedExtent" =>
        createCollection[TemporalProjectedExtent, SpaceTimeKey](
          returnedRdd,
          schemaJson,
          pythonExtent,
          pythonTileLayout,
          outputCrs)
    }

  private def createPyramidCollection[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
    K2: Boundable: SpatialComponent: JsonFormat
  ](
    returnedRdd: JavaRDD[Array[Byte]],
    schemaJson: String,
    zoomedLayoutScheme: ZoomedLayoutScheme,
    maxZoom: Int,
    outputCrs: String
  ): (Int, String) = {
    val rdd = PythonTranslator.fromPython[(K, MultibandTile)](returnedRdd, Some(schemaJson))

    val (zoomLevel, metadata) =
      outputCrs match {
        case "" => TileLayerMetadata.fromRdd(rdd, zoomedLayoutScheme, maxZoom)
        case projection => TileLayerMetadata.fromRdd(rdd, CRS.fromName(projection), zoomedLayoutScheme, maxZoom)
      }

    (zoomLevel, metadata.toJson.compactPrint)
  }

  def collectPythonMetadata(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schemaJson: String,
    crs: String,
    tileSize: Int,
    resolutionThreshold: Double,
    maxZoom: Int,
    outputCrs: String
  ): (Int, String) =
      keyType match {
        case "ProjectedExtent" =>
          createPyramidCollection[ProjectedExtent, SpatialKey](
            returnedRDD,
            schemaJson,
            ZoomedLayoutScheme(CRS.fromName(crs), tileSize, resolutionThreshold),
            maxZoom,
            outputCrs)
        case "TemporalProjectedExtent" =>
          createPyramidCollection[TemporalProjectedExtent, SpaceTimeKey](
            returnedRDD,
            schemaJson,
            ZoomedLayoutScheme(CRS.fromName(crs), tileSize, resolutionThreshold),
            maxZoom,
            outputCrs)
      }

  private def createPyramidCollection[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
    K2: Boundable: SpatialComponent: JsonFormat
  ](
    returnedRdd: JavaRDD[Array[Byte]],
    schemaJson: String,
    floatingLayoutScheme: FloatingLayoutScheme,
    outputCrs: String
  ): (Int, String) = {
    val rdd = PythonTranslator.fromPython[(K, MultibandTile)](returnedRdd, Some(schemaJson))

    val (zoomLevel, metadata) =
      outputCrs match {
        case "" => TileLayerMetadata.fromRdd(rdd, floatingLayoutScheme)
        case projection => TileLayerMetadata.fromRdd(rdd, CRS.fromName(projection), floatingLayoutScheme)
      }

    (zoomLevel, metadata.toJson.compactPrint)
  }

  def collectPythonMetadata(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schemaJson: String,
    tileCols: Int,
    tileRows: Int,
    outputCrs: String
  ): (Int, String) =
      keyType match {
        case "ProjectedExtent" =>
          createPyramidCollection[ProjectedExtent, SpatialKey](
            returnedRDD,
            schemaJson,
            FloatingLayoutScheme(tileCols, tileRows),
            outputCrs)
        case "TemporalProjectedExtent" =>
          createPyramidCollection[TemporalProjectedExtent, SpaceTimeKey](
            returnedRDD,
            schemaJson,
            FloatingLayoutScheme(tileCols, tileRows),
            outputCrs)
      }
}
