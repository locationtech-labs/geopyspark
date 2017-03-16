package geopyspark.geotrellis.spark.pyramid

import geopyspark.geotrellis._
import geopyspark.geotrellis.spark.tiling._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.raster.merge._
import geotrellis.raster.prototype._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.pyramid._
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


object PyramidWrapper {
  private def buildPyramid[
    K: SpatialComponent: AvroRecordCodec: ClassTag: JsonFormat,
    M: Component[?, LayoutDefinition]: Component[?, Bounds[K]]
  ](
    returnedRDD: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    tileSize: Int,
    resolutionThreshold: Double,
    startZoom: Int,
    endZoom: Int,
    options: java.util.Map[String, String]
  ): java.util.List[(Int, (JavaRDD[Array[Byte]], String), String)] = {
    val rdd: RDD[(K, MultibandTile)] =
      PythonTranslator.fromPython[(K, MultibandTile)](returnedRDD, Some(schema))

    val metadataAST = returnedMetadata.parseJson
    val metadata = metadataAST.convertTo[TileLayerMetadata[K]]

    val contextRDD = ContextRDD(rdd, metadata)

    val zoomedLayout = ZoomedLayoutScheme(metadata.crs, tileSize, resolutionThreshold)

    val pyramidOptions = {
      val scalaMap = options.asScala

      if (scalaMap.isEmpty)
        Pyramid.Options.DEFAULT
      else {
        val resampleMethod =
          TilerOptions.getResampleMethod(scalaMap.get("resampleMethod"))

        Pyramid.Options(resampleMethod=resampleMethod)
      }
    }

    val leveledList =
      Pyramid.levelStream(contextRDD, zoomedLayout, startZoom, endZoom, pyramidOptions).toList

    leveledList.map{
      x => (x._1, PythonTranslator.toPython(x._2), x._2.metadata.toJson.compactPrint)
    }.asJava
  }

  def buildPythonPyramid(
    keyType: String,
    returnedRDD: RDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    tileSize: Int,
    resolutionThreshold: Double,
    startZoom: Int,
    endZoom: Int,
    options: java.util.Map[String, String]
  ): java.util.List[(Int, (JavaRDD[Array[Byte]], String), String)] =
    keyType match {
      case "SpatialKey" =>
        buildPyramid[SpatialKey, TileLayerMetadata[SpatialKey]](
          returnedRDD,
          schema,
          returnedMetadata,
          tileSize,
          resolutionThreshold,
          startZoom,
          endZoom,
          options)
      case "SpaceTimeKey" =>
        buildPyramid[SpaceTimeKey, TileLayerMetadata[SpaceTimeKey]](
          returnedRDD,
          schema,
          returnedMetadata,
          tileSize,
          resolutionThreshold,
          startZoom,
          endZoom,
          options)
    }
}
