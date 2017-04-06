package geopyspark.geotrellis.spark.stitch

import geopyspark.geotrellis._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.stitch.TileLayoutStitcher

import spray.json._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import scala.reflect.ClassTag


object StitchWrapper {

  def stitch(
    javaRdd: JavaRDD[Array[Byte]],
    schema: String,
    metadataStr: String
  ): (Array[Byte], String) = {
    val metadataAST = metadataStr.parseJson
    val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
    val rdd = ContextRDD(
      PythonTranslator
        .fromPython[(SpatialKey, MultibandTile)](javaRdd, Some(schema))
        .map({ case (k, v) => (k, v.band(0)) }),
      metadata
    )
    val tile: Tile = rdd.stitch.tile

    PythonTranslator.toPython[Tile](tile)
  }

}
