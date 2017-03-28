package geopyspark.geotrellis.spark.reproject

import geopyspark.geotrellis._

import geotrellis.util._
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
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) = {
    val rdd: RDD[(K, MultibandTile)] =
      PythonTranslator.fromPython[(K, MultibandTile)](returnedRDD, Some(schema))

    val contextRDD = ContextRDD(rdd, metadata)
    val crs = CRS.fromName(destCRS)
    val options = Reproject.Options(matchLayerExtent=matchLayerExtent)

    val (zoom, reprojectedRDD) = TileRDDReproject(contextRDD, crs, layout, options)

    (zoom, PythonTranslator.toPython(reprojectedRDD), metadata.toJson.compactPrint)
  }

  def reproject(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    matchLayerExtent: Boolean
  ): (Int, (JavaRDD[Array[Byte]], String), String) =
    keyType match {
      case "SpatialKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        val layout = Right(metadata.layout)

        reprojectRDD[SpatialKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent)
      }
      case "SpaceTimeKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        val layout = Right(metadata.layout)

        reprojectRDD[SpaceTimeKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent)
      }
    }

  def reprojectWithZoom(
    keyType: String,
    returnedRDD: JavaRDD[Array[Byte]],
    schema: String,
    returnedMetadata: String,
    destCRS: String,
    matchLayerExtent: Boolean,
    tileSize: Int,
    resolutionThreshold: Double
  ): (Int, (JavaRDD[Array[Byte]], String), String) =
    keyType match {
      case "SpatialKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        val layout = Left(ZoomedLayoutScheme(metadata.crs, tileSize, resolutionThreshold))

        reprojectRDD[SpatialKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent)
      }
      case "SpaceTimeKey" => {
        val metadataAST = returnedMetadata.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        val layout = Left(ZoomedLayoutScheme(metadata.crs, tileSize, resolutionThreshold))

        reprojectRDD[SpaceTimeKey](returnedRDD, schema, metadata, destCRS, layout, matchLayerExtent)
      }
    }
}
