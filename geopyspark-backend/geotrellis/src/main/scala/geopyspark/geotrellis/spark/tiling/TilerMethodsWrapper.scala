package geopyspark.geotrellis.spark.tiling

import geopyspark.geotrellis._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.tiling._

import spray.json._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map

import scala.reflect.ClassTag


object TilerMethodsWrapper {
  private def cutRDDTiles[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
    V <: CellGrid: AvroRecordCodec: ClassTag: (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V]),
    K2: Boundable: SpatialComponent: ClassTag: AvroRecordCodec: JsonFormat
  ](
    returnedRdd: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    returnedResampleMethod: String
  ): (JavaRDD[Array[Byte]], String) = {
    val rdd: RDD[(K, V)] =
      PythonTranslator.fromPython[(K, V)](returnedRdd, Some(schema))

    val metadataAST = returnedMetadata.parseJson
    val metadata = metadataAST.convertTo[TileLayerMetadata[K2]]

    val cutRdd = {
        val resampleMethod =
          TilerOptions.getResampleMethod(returnedResampleMethod)
        rdd.cutTiles(metadata, resampleMethod)
      }

    PythonTranslator.toPython[(K2, V)](cutRdd)
  }

  def cutTiles(
    keyType: String,
    returnedRdd: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    returnedResampleMethod: String
  ): (JavaRDD[Array[Byte]], String) =
    keyType match {
      case "ProjectedExtent" =>
        cutRDDTiles[ProjectedExtent, MultibandTile, SpatialKey](
          returnedRdd,
          schema,
          returnedMetadata,
          returnedResampleMethod)
      case "TemporalProjectedExtent" =>
        cutRDDTiles[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](
          returnedRdd,
          schema,
          returnedMetadata,
          returnedResampleMethod)
    }

  private def toLayout[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
    V <: CellGrid: AvroRecordCodec: ClassTag: (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V]),
    K2: Boundable: SpatialComponent: ClassTag: AvroRecordCodec: JsonFormat
  ](
    returnedRdd: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    resampleMethod: String
  ): (JavaRDD[Array[Byte]], String) = {
    val rdd: RDD[(K, V)] = PythonTranslator.fromPython[(K, V)](returnedRdd, Some(schema))

    val metadataAST = returnedMetadata.parseJson
    val metadata = metadataAST.convertTo[TileLayerMetadata[K2]]

    val returnedOptions = TilerOptions.setValues(resampleMethod)
    val result = rdd.tileToLayout(metadata, returnedOptions)

    PythonTranslator.toPython(result)
  }

  def tileToLayout(
    keyType: String,
    returnedRdd: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    resampleMethod: String
  ): (JavaRDD[Array[Byte]], String) =
    keyType match {
      case "ProjectedExtent" =>
        toLayout[ProjectedExtent, MultibandTile, SpatialKey](
          returnedRdd,
          schema,
          returnedMetadata,
          resampleMethod)
      case "TemporalProjectedExtent" =>
        toLayout[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](
          returnedRdd,
          schema,
          returnedMetadata,
          resampleMethod)
    }
}
