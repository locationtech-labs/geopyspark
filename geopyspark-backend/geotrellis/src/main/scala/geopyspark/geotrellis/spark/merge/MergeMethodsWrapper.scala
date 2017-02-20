package geopyspark.geotrellis.spark.merge

import geopyspark.geotrellis._

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.merge._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._

import spray.json._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag


object MergeMethodsWrapper {
  private def _merge[
    K: AvroRecordCodec: ClassTag,
    V <: CellGrid: AvroRecordCodec: ClassTag: (? => TileMergeMethods[V])
  ](
    self: RDD[Array[Byte]],
    selfSchema: String,
    other: RDD[Array[Byte]],
    otherSchema: String
  ): (JavaRDD[Array[Byte]], String) = {
    val rdd1: RDD[(K, V)] = PythonTranslator.fromPython[(K, V)](self, Some(selfSchema))
    val rdd2: RDD[(K, V)] = PythonTranslator.fromPython[(K, V)](other, Some(otherSchema))

    val result = rdd1.merge(rdd2)

    PythonTranslator.toPython[(K, V)](result)
  }

  def merge(
    keyType: String,
    valueType: String,
    self: RDD[Array[Byte]],
    selfSchema: String,
    other: RDD[Array[Byte]],
    otherSchema: String
  ): (JavaRDD[Array[Byte]], String) =
    (keyType, valueType) match {
      case ("spatial", "singleband") =>
        _merge[ProjectedExtent, Tile](
          self,
          selfSchema,
          other,
          otherSchema)
      case ("spatial", "multiband") =>
        _merge[ProjectedExtent, MultibandTile](
          self,
          selfSchema,
          other,
          otherSchema)
      case ("spacetime", "singleband") =>
        _merge[TemporalProjectedExtent, Tile](
          self,
          selfSchema,
          other,
          otherSchema)
      case ("spacetime", "multiband") =>
        _merge[TemporalProjectedExtent, MultibandTile](
          self,
          selfSchema,
          other,
          otherSchema)
    }
}
