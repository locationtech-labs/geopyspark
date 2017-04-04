package geopyspark.geotrellis.spark.reproject

import geopyspark.geotrellis._
import geopyspark.geotrellis.GeoTrellisUtils._
import geopyspark.geotrellis.spark.tiling._

import geotrellis.util._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._

import spray.json._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag

object ReprojectWrapper {
  private def reprojectRDD(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    layout: Either[LayoutScheme, LayoutDefinition],
    matchLayerExtent: Boolean,
    resampleMethod: String
  ): (Int, (JavaRDD[Array[Byte]], String), String) = {
    val metadataAST = returnedMetadata.parseJson
    val crs = CRS.fromName(destCRS)

    val options = {
      val method = TilerOptions.getResampleMethod(resampleMethod)

      Reproject.Options(geotrellis.raster.reproject.Reproject.Options(method=method),
        matchLayerExtent=matchLayerExtent)
    }

    keyType match {
      case "SpatialKey" => {
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]

        val rdd: RDD[(SpatialKey, MultibandTile)] =
          PythonTranslator.fromPython[(SpatialKey, MultibandTile)](returnedRDD, Some(schema))

        val context = ContextRDD(rdd, metadata)

        val (zoom, reprojectedRDD) =
          layout match {
            case Left(scheme) => context.reproject(crs, scheme, options)
            case Right(layout) => context.reproject(crs, layout, options)
          }

        (zoom, PythonTranslator.toPython(reprojectedRDD), reprojectedRDD.metadata.toJson.compactPrint)
      }

      case "SpaceTimeKey" => {
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]

        val rdd: RDD[(SpaceTimeKey, MultibandTile)] =
          PythonTranslator.fromPython[(SpaceTimeKey, MultibandTile)](returnedRDD, Some(schema))

        val context = ContextRDD(rdd, metadata)

        val (zoom, reprojectedRDD) =
          layout match {
            case Left(scheme) => context.reproject(crs, scheme, options)
            case Right(layout) => context.reproject(crs, layout, options)
          }

        (zoom, PythonTranslator.toPython(reprojectedRDD), reprojectedRDD.metadata.toJson.compactPrint)
      }
    }
  }

  def reproject(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    layoutExtent: java.util.Map[String, Double],
    tileLayout: java.util.Map[String, Int],
    resampleMethod: String,
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) = {
      val layout = Right(LayoutDefinition(layoutExtent.toExtent, tileLayout.toTileLayout))

      reprojectRDD(keyType,
        returnedRDD,
        schema,
        returnedMetadata,
        destCRS,
        layout,
        matchLayerExtent,
        resampleMethod)
    }

  def reproject(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    tileSize: Int,
    resolutionThreshold: Double,
    resampleMethod: String,
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) = {
    val layout = Left(ZoomedLayoutScheme(CRS.fromName(destCRS), tileSize, resolutionThreshold))

    reprojectRDD(keyType,
      returnedRDD,
      schema,
      returnedMetadata,
      destCRS,
      layout,
      matchLayerExtent,
      resampleMethod)
  }

  def reproject(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    tileSize: Int,
    resampleMethod: String,
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) = {
    val layout = Left(FloatingLayoutScheme(tileSize))

    reprojectRDD(keyType,
      returnedRDD,
      schema,
      returnedMetadata,
      destCRS,
      layout,
      matchLayerExtent,
      resampleMethod)
  }

  def reproject(
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    destCRS: String,
    resampleMethod: String,
    errorThreshold: Double
  ): (JavaRDD[Array[Byte]], String) = {
    val rdd =
      PythonTranslator.fromPython[(ProjectedExtent, MultibandTile)](returnedRDD, Some(schema))

    val crs = CRS.fromName(destCRS)

    val options = {
      val method = TilerOptions.getResampleMethod(resampleMethod)

      geotrellis.raster.reproject.Reproject.Options(method=method, errorThreshold=errorThreshold)
    }

    val result = ProjectedExtentComponentReproject(rdd, crs, options)

    PythonTranslator.toPython(result)
  }
}
