package geopyspark.geotrellis.spark

import geopyspark.geotrellis._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.tiling._

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map


object TileLayerMetadataWrapper {
  private def formatMetadata[K: Boundable: SpatialComponent]
  (metadata: TileLayerMetadata[K]): java.util.Map[String, Any] = {
    val extent = (metadata.extent.xmin,
      metadata.extent.ymin,
      metadata.extent.xmax,
      metadata.extent.ymax)

    val tileLayout = metadata.layout.tileLayout

    val map: scala.collection.immutable.Map[String, Any] =
      scala.collection.immutable.Map("cellType" -> metadata.cellType.name,
      "layout" -> (extent, (tileLayout.layoutCols,
        tileLayout.layoutRows,
        tileLayout.tileCols,
        tileLayout.tileRows)),
    "extent" -> extent,
    "crs" -> metadata.crs.toProj4String)

    mapAsJavaMap(map)
  }

  private def createLayoutDefinition(
    pythonExtent: (Int, Int, Int, Int),
    pythonTileLayout: (Int, Int, Int, Int)
  ): LayoutDefinition = {

    val extent = Extent(
      pythonExtent._1,
      pythonExtent._2,
      pythonExtent._3,
      pythonExtent._4)

    val tileLayout = TileLayout(
      pythonTileLayout._1,
      pythonTileLayout._2,
      pythonTileLayout._3,
      pythonTileLayout._4)

    LayoutDefinition(extent, tileLayout)
  }

  private def createCollection[
    K: GetComponent[?, ProjectedExtent]: (? => TilerKeyMethods[K, K2]),
    T <: CellGrid,
    K2: Boundable: SpatialComponent
  ](
    rdd: RDD[(K, T)],
    pythonExtent: (Int, Int, Int, Int),
    pythonTileLayout: (Int, Int, Int, Int),
    crsJavaMap: java.util.Map[_ <: String, _ <: String]
  ): java.util.Map[String, Any] = {

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

  def collectSpatialSinglebandMetadata(
    javaRDD: RDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: (Int, Int, Int, Int),
    pythonTileLayout: (Int, Int, Int, Int),
    crsJavaMap: java.util.Map[_ <: String, _ <: String]
    ): java.util.Map[String, Any] = {
    val rdd =
      PythonTranslator.fromPython[(ProjectedExtent, Tile)](javaRDD, Some(schemaJson))

    createCollection(rdd, pythonExtent, pythonTileLayout, crsJavaMap)
  }

  def collectSpatialMultibandMetadata(
    javaRDD: RDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: (Int, Int, Int, Int),
    pythonTileLayout: (Int, Int, Int, Int),
    crsJavaMap: java.util.Map[_ <: String, _ <: String]
    ): java.util.Map[String, Any] = {
    val rdd =
      PythonTranslator.fromPython[(ProjectedExtent, MultibandTile)](javaRDD, Some(schemaJson))

    createCollection(rdd, pythonExtent, pythonTileLayout, crsJavaMap)
  }

  def collectSpaceTimeSinglebandMetadata(
    javaRDD: RDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: (Int, Int, Int, Int),
    pythonTileLayout: (Int, Int, Int, Int),
    crsJavaMap: java.util.Map[_ <: String, _ <: String]
    ): java.util.Map[String, Any] = {
    val rdd =
      PythonTranslator.fromPython[(TemporalProjectedExtent, Tile)](javaRDD, Some(schemaJson))

    createCollection[TemporalProjectedExtent, Tile, SpaceTimeKey](rdd, pythonExtent, pythonTileLayout, crsJavaMap)
  }

  def collectSpaceTimeMultibandMetadata(
    javaRDD: RDD[Array[Byte]],
    schemaJson: String,
    pythonExtent: (Int, Int, Int, Int),
    pythonTileLayout: (Int, Int, Int, Int),
    crsJavaMap: java.util.Map[_ <: String, _ <: String]
    ): java.util.Map[String, Any] = {
    val rdd =
      PythonTranslator.fromPython[(TemporalProjectedExtent, MultibandTile)](javaRDD, Some(schemaJson))

    createCollection[TemporalProjectedExtent, MultibandTile, SpaceTimeKey](rdd, pythonExtent, pythonTileLayout, crsJavaMap)
  }
}
