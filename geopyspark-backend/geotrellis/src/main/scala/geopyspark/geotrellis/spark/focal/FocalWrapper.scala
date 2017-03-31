package geopyspark.geotrellis.spark.focal

import geopyspark.geotrellis._

import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.mapalgebra.focal._

import spray.json._

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object FocalWrapper {
  private def focal[K: SpatialComponent: ClassTag: AvroRecordCodec: JsonFormat](
    javaRdd: JavaRDD[Array[Byte]],
    schema: String,
    metadata: TileLayerMetadata[K],
    _op: String,
    _neighborhood: String,
    _param1: Double, _param2: Double, _param3: Double
  ): (JavaRDD[Array[Byte]], String) = {
    val _rdd = PythonTranslator.fromPython[(K, MultibandTile)](javaRdd, Some(schema))
      val rdd: RDD[(K, Tile)] = ContextRDD(
        _rdd.map({ case (k, v) => (k, v.band(0)) }),
        metadata
      )
    val neighborhood = _neighborhood match {
      case "annulus" => Annulus(_param1, _param2)
      case "nesw" => Nesw(_param1.toInt)
      case "square" => Square(_param1.toInt)
      case "wedge" => Wedge(_param1, _param2, _param3)
      case _ => throw new Exception
    }
    val target = TargetCell.All
    val op: ((Tile, Option[GridBounds]) => Tile) = _op match {
      case "Sum" => { (tile, bounds) => Sum(tile, neighborhood, bounds, target) }
      case "Min" => { (tile, bounds) => Min(tile, neighborhood, bounds, target) }
      case "Max" => { (tile, bounds) => Max(tile, neighborhood, bounds, target) }
      case "Mean" => { (tile, bounds) => Mean(tile, neighborhood, bounds, target) }
      case "Median" => { (tile, bounds) => Median(tile, neighborhood, bounds, target) }
      case "Mode" => { (tile, bounds) => Mode(tile, neighborhood, bounds, target) }
      case "StandardDeviation" => { (tile, bounds) => StandardDeviation(tile, neighborhood, bounds, target) }
      case _ => throw new Exception
    }

    PythonTranslator.toPython(FocalOperation(rdd, neighborhood)(op))
  }

  def focal(
    keyType: String,
    javaRdd: JavaRDD[Array[Byte]],
    schema: String,
    metadataStr: String,
    op: String,
    neighborhood: String,
    param1: Double, param2: Double, param3: Double
  ): (JavaRDD[Array[Byte]], String) = {
    keyType match {
      case "SpatialKey" => {
        val metadataAST = metadataStr.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        focal[SpatialKey](javaRdd, schema, metadata, op, neighborhood, param1, param2, param3)
      }
      case "SpaceTimeKey" => {
        val metadataAST = metadataStr.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        focal[SpaceTimeKey](javaRdd, schema, metadata, op, neighborhood, param1, param2, param3)
      }
    }
  }

}
