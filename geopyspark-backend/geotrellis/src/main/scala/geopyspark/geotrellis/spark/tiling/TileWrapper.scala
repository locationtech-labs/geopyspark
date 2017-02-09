package geopyspark.geotrellis.spark.tiling

import geopyspark.geotrellis._

import geotrellis.util._
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.resample._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.tiling._

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map


object TilerOptions {
  def default = Tiler.Options.DEFAULT

  def setValues(javaMap: java.util.Map[String, Any]): Tiler.Options = {
    val stringValues = Array("resampleMethod", "partitioner")
    val scalaMap = javaMap.asScala

    val intMap =
      scalaMap.filterKeys(x => !(stringValues.contains(x)))
        .mapValues(x => x.asInstanceOf[Int])

    val stringMap =
      scalaMap.filterKeys(x => stringValues.contains(x))
        .mapValues(x => x.asInstanceOf[String])

    val resampleMethod: ResampleMethod =
      stringMap.get("resampleMethod") match {
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


object TileWrapper {
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
