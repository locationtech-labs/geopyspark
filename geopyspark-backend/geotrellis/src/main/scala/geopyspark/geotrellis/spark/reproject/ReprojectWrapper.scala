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
  private def reprojectRDD[
    K: SpatialComponent: ClassTag: Boundable: AvroRecordCodec: JsonFormat
  ](
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    metadata: TileLayerMetadata[K],
    destCRS: String,
    layout: Either[LayoutScheme, LayoutDefinition],
    matchLayerExtent: Boolean,
    returnedResampleMethod: String
  ): (Int, (JavaRDD[Array[Byte]], String), String) = {
    val rdd: RDD[(K, MultibandTile)] =
      PythonTranslator.fromPython[(K, MultibandTile)](returnedRDD, Some(schema))

    val contextRDD = ContextRDD(rdd, metadata)
    val crs = CRS.fromName(destCRS)

    val options = {
      val resampleMethod = TilerOptions.getResampleMethod(returnedResampleMethod)

      Reproject.Options(geotrellis.raster.reproject.Reproject.Options(method=resampleMethod),
        matchLayerExtent=matchLayerExtent)
    }

    val (zoom, reprojectedRDD) =
      layout match {
        case Left(scheme) => contextRDD.reproject(crs, scheme, options)
        case Right(layout) => contextRDD.reproject(crs, layout, options)
      }

    (zoom, PythonTranslator.toPython(reprojectedRDD), reprojectedRDD.metadata.toJson.compactPrint)
  }

  def reproject(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    layoutExtent: java.util.Map[String, Double],
    tileLayout: java.util.Map[String, Int],
    returnedResampleMethod: String,
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) =
    keyType match {
      case "SpatialKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        val layout = Right(LayoutDefinition(layoutExtent.toExtent, tileLayout.toTileLayout))

        reprojectRDD[SpatialKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent, returnedResampleMethod)
      }
      case "SpaceTimeKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        val layout = Right(LayoutDefinition(layoutExtent.toExtent, tileLayout.toTileLayout))

        reprojectRDD[SpaceTimeKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent, returnedResampleMethod)
      }
    }

  def reproject(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    tileSize: Int,
    resolutionThreshold: Double,
    returnedResampleMethod: String,
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) =
    keyType match {
      case "SpatialKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        val layout = Left(ZoomedLayoutScheme(CRS.fromName(destCRS), tileSize, resolutionThreshold))

        reprojectRDD[SpatialKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent, returnedResampleMethod)
      }
      case "SpaceTimeKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        val layout = Left(ZoomedLayoutScheme(CRS.fromName(destCRS), tileSize, resolutionThreshold))

        reprojectRDD[SpaceTimeKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent, returnedResampleMethod)
      }
    }

  def reproject(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    tileSize: Int,
    returnedResampleMethod: String,
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) =
    keyType match {
      case "SpatialKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        val layout = Left(FloatingLayoutScheme(tileSize))

        reprojectRDD[SpatialKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent, returnedResampleMethod)
      }
      case "SpaceTimeKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        val layout = Left(FloatingLayoutScheme(tileSize))

        reprojectRDD[SpaceTimeKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent, returnedResampleMethod)
      }
    }

  def reproject(
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    destCRS: String,
    returnedResampleMethod: String,
    errorThreshold: Double
  ): (JavaRDD[Array[Byte]], String) = {
    val rdd =
      PythonTranslator.fromPython[(ProjectedExtent, MultibandTile)](returnedRDD, Some(schema))

    val crs = CRS.fromName(destCRS)

    val options = {
      val resampleMethod = TilerOptions.getResampleMethod(returnedResampleMethod)

      geotrellis.raster.reproject.Reproject.Options(method=resampleMethod,
        errorThreshold=errorThreshold)
    }

    val result = ProjectedExtentComponentReproject(rdd, crs, options)

    PythonTranslator.toPython(result)
  }
}
