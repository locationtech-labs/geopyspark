package geopyspark.geotrellis.spark.tiling

import geopyspark.geotrellis._
import geopyspark.geotrellis.GeoTrellisUtils._

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
  private def _cutTiles[
  K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
  V <: CellGrid: AvroRecordCodec: ClassTag: (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V]),
  K2: Boundable: SpatialComponent: ClassTag: AvroRecordCodec: JsonFormat
  ](
    returnedRdd: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    resampleMap: java.util.Map[String, String]
  ): (JavaRDD[Array[Byte]], String) = {
    val rdd: RDD[(K, V)] = PythonTranslator.fromPython[(K, V)](returnedRdd, Some(schema))

    val metadataAST = returnedMetadata.parseJson
    val metadata = metadataAST.convertTo[TileLayerMetadata[K2]]

    val scalaMap = resampleMap.asScala

    val cutRdd =
      if (scalaMap.isEmpty)
        rdd.cutTiles(metadata)
      else {
        val resampleMethod = TilerOptions
          .getResampleMethod(scalaMap.get("resampleMethod"))

        rdd.cutTiles(metadata, resampleMethod)
      }

    PythonTranslator.toPython[(K2, V)](cutRdd)
  }

  def cutTiles(
    keyType: String,
    valueType: String,
    returnedRdd: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    resampleMap: java.util.Map[String, String]
  ): (JavaRDD[Array[Byte]], String) =
    (keyType, valueType) match {
      case ("spatial", "singleband") =>
        _cutTiles[ProjectedExtent, Tile, SpatialKey](
          returnedRdd,
          schema,
          returnedMetadata,
          resampleMap)
      case ("spatial", "multiband") =>
        _cutTiles[ProjectedExtent, MultibandTile, SpatialKey](
          returnedRdd,
          schema,
          returnedMetadata,
          resampleMap)
      case ("spacetime", "singleband") =>
        _cutTiles[TemporalProjectedExtent, Tile, SpaceTimeKey](
          returnedRdd,
          schema,
          returnedMetadata,
          resampleMap)
      case ("spacetime", "multiband") =>
        _cutTiles[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](
          returnedRdd,
          schema,
          returnedMetadata,
          resampleMap)
    }
}
