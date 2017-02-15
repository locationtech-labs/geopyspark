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
import geotrellis.spark.io.avro._
import geotrellis.spark.tiling._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map

import scala.reflect.ClassTag


object TilerOptions {
  def default = Tiler.Options.DEFAULT

  def getResampleMethod(resampleMethod: Option[String]): ResampleMethod =
    resampleMethod match {
      case None => default.resampleMethod
      case Some(x) =>
        if (x == "NearestNeighbor")
          NearestNeighbor
        else if (x == "Bilinear")
          Bilinear
        else if (x == "CubicConvolution")
          CubicConvolution
        else if (x == "CubicSpline")
          CubicSpline
        else if (x == "Lanczos")
          Lanczos
        else if (x == "Average")
          Average
        else if (x == "Mode")
          Mode
        else if (x == "Median")
          Median
        else if (x == "Max")
          Max
        else if (x == "Min")
          Min
        else
          throw new Exception(s"$x, Is not a known sampling method")
    }

  def setValues(javaMap: java.util.Map[String, Any]): Tiler.Options = {
    val stringValues = Array("resampleMethod", "partitioner")
    val scalaMap = javaMap.asScala

    val intMap =
      scalaMap.filterKeys(x => !(stringValues.contains(x)))
        .mapValues(x => x.asInstanceOf[Int])

    val stringMap =
      scalaMap.filterKeys(x => stringValues.contains(x))
        .mapValues(x => x.asInstanceOf[String])

    val resampleMethod = getResampleMethod(stringMap.get("resampleMethod"))

    val partitioner: Option[Partitioner] =
      stringMap.get("partitioer") match {
        case None => None
        case Some(x) =>
          intMap.get("numPartitions") match {
            case None => None
            case Some(num) =>
              if (x == "HashPartitioner")
                Some(new HashPartitioner(num))
              else
                throw new Exception(s"$x, Is not a known Partitioner")
          }
      }

    Tiler.Options(
      resampleMethod = resampleMethod,
      partitioner = partitioner)
  }
}


object TilerMethods {
  private def _cutTiles[
  K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
  V <: CellGrid: AvroRecordCodec: ClassTag: (? => TileMergeMethods[V]): (? => TilePrototypeMethods[V]),
  K2: Boundable: SpatialComponent: ClassTag: AvroRecordCodec
  ](
    returnedRdd: RDD[Array[Byte]],
    schema: String,
    pythonMetadata: java.util.Map[String, Any],
    resampleMap: java.util.Map[String, String]
  ): (JavaRDD[Array[Byte]], String) = {
    val rdd: RDD[(K, V)] = PythonTranslator.fromPython[(K, V)](returnedRdd, Some(schema))
    val layoutDefinition: LayoutDefinition = pythonMetadata.getLayoutDefinition
    val crs: CRS = CRS.fromString(pythonMetadata("crs").asInstanceOf[String])

    val metadata = rdd.collectMetadata[K2](crs, layoutDefinition)

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
    pythonMetadata: java.util.Map[String, Any],
    resampleMap: java.util.Map[String, String]
  ): (JavaRDD[Array[Byte]], String) =
    (keyType, valueType) match {
      case ("spatial", "singleband") =>
        _cutTiles[ProjectedExtent, Tile, SpatialKey](
          returnedRdd,
          schema,
          pythonMetadata,
          resampleMap)
      case ("spatial", "multiband") =>
        _cutTiles[ProjectedExtent, MultibandTile, SpatialKey](
          returnedRdd,
          schema,
          pythonMetadata,
          resampleMap)
      case ("spacetime", "singleband") =>
        _cutTiles[TemporalProjectedExtent, Tile, SpaceTimeKey](
          returnedRdd,
          schema,
          pythonMetadata,
          resampleMap)
      case ("spacetime", "multiband") =>
        _cutTiles[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](
          returnedRdd,
          schema,
          pythonMetadata,
          resampleMap)
    }
}
