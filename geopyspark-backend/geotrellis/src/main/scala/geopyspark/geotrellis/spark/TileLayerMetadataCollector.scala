package geopyspark.geotrellis.spark

import geopyspark.geotrellis._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.io.avro._
import geotrellis.spark.tiling._

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map

import scala.reflect.ClassTag


object TileLayerMetadataCollector {
  private def formatMetadata[K: Boundable: SpatialComponent]
  (metadata: TileLayerMetadata[K]): java.util.Map[String, Any] = {
    val extent = metadata.extent
    val tileLayout = metadata.layout.tileLayout

    val map: scala.collection.immutable.Map[String, Any] =
      scala.collection.immutable.Map(
        "cellType" -> metadata.cellType.name,
        "xmin" -> extent.xmin,
        "ymin" -> extent.ymin,
        "xmax" -> extent.xmax,
        "ymax" -> extent.ymax,
        "layoutCols" -> tileLayout.layoutCols,
        "layoutRows" -> tileLayout.layoutRows,
        "tileCols" -> tileLayout.tileCols,
        "tileRows" -> tileLayout.tileRows,
        "crs" -> metadata.crs.toProj4String)

    mapAsJavaMap(map)
  }

  private def createLayoutDefinition(
    pythonExtent: java.util.ArrayList[Double],
    pythonTileLayout: java.util.ArrayList[Int]
  ): LayoutDefinition = {

    val extent = Extent(
      pythonExtent(0),
      pythonExtent(1),
      pythonExtent(2),
      pythonExtent(3))

    val tileLayout = TileLayout(
      pythonTileLayout(0),
      pythonTileLayout(1),
      pythonTileLayout(2),
      pythonTileLayout(3))

    LayoutDefinition(extent, tileLayout)
  }

  private def createCollection[
  K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]): AvroRecordCodec: ClassTag,
  T <: CellGrid: AvroRecordCodec: ClassTag,
  K2: Boundable: SpatialComponent
  ](
    javaRDD: RDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: java.util.ArrayList[Double],
    pythonTileLayout: java.util.ArrayList[Int],
    crsJavaMap: java.util.Map[_ <: String, _ <: String]
  ): java.util.Map[String, Any] = {

    val rdd =
      PythonTranslator.fromPython[(K, T)](javaRDD, Some(schemaJson))

    val layoutDefinition = createLayoutDefinition(pythonExtent, pythonTileLayout)
    val crsScalaMap = crsJavaMap.asScala

    val crs: Option[CRS] =
      if (crsScalaMap.isEmpty)
        None
      else
        crsScalaMap.head match {
          case ("projParams", projParams) => Some(CRS.fromString(projParams))
          case ("epsg", epsgString) => Some(CRS.fromName(s"EPSG:$epsgString"))
          case ("wktString", wktString) => Some(CRS.fromWKT(wktString))
          case (k, _) => throw new Exception(s"Cannot create CRS from $k")
        }

        val metadata =
          crs match {
            case None => rdd.collectMetadata[K2](layoutDefinition)
            case Some(x) => rdd.collectMetadata[K2](x, layoutDefinition)
          }

        formatMetadata(metadata)
  }

  def collectPythonMetadata(
    valueType: String,
    keyType: String,
    javaRDD: RDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: java.util.ArrayList[Double],
    pythonTileLayout: java.util.ArrayList[Int],
    crsJavaMap: java.util.Map[_ <: String, _ <: String]
  ): java.util.Map[String, Any] = {

    (valueType, keyType) match {
      case ("spatial", "singleband") =>
        createCollection[ProjectedExtent, Tile, SpatialKey](
          javaRDD,
          schemaJson,
          pythonExtent,
          pythonTileLayout,
          crsJavaMap)
        case ("spatial", "multiband") =>
          createCollection[ProjectedExtent, MultibandTile, SpatialKey](
            javaRDD,
            schemaJson,
            pythonExtent,
            pythonTileLayout,
            crsJavaMap)
          case ("spacetime", "singleband") =>
            createCollection[TemporalProjectedExtent, Tile, SpaceTimeKey](
              javaRDD,
              schemaJson,
              pythonExtent,
              pythonTileLayout,
              crsJavaMap)
            case ("spacetime", "multiband") =>
              createCollection[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](
                javaRDD,
                schemaJson,
                pythonExtent,
                pythonTileLayout,
                crsJavaMap)
    }
  }
}
