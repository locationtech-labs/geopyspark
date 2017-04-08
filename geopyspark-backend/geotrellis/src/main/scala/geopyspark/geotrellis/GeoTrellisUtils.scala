package geopyspark.geotrellis

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._

import org.apache.spark.rdd._

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map


object GeoTrellisUtils {
  def convertToScalaMap(
    javaMap: java.util.Map[String, Any],
    stringValues: Array[String]
  ): (scala.collection.Map[String, String], scala.collection.Map[String, Int]) = {
    val scalaMap = javaMap.asScala

    val intMap =
      scalaMap.filterKeys(x => !(stringValues.contains(x)))
        .mapValues(x => x.asInstanceOf[Int])

    val stringMap =
      scalaMap.filterKeys(x => stringValues.contains(x))
        .mapValues(x => x.asInstanceOf[String])

    (stringMap, intMap)
  }

  def getReprojectOptions(resampleMethod: String): Reproject.Options = {
    import Reproject.Options

    val method = TileRDD.getResampleMethod(resampleMethod)

    Options(geotrellis.raster.reproject.Reproject.Options(method=method))
  }

  implicit class JavaMapExtensions(m: java.util.Map[String, _]) {
    def toExtent: Extent = {
      val mappedExtent = m.mapValues(x => x.asInstanceOf[Double])
      Extent(
        mappedExtent("xmin"),
        mappedExtent("ymin"),
        mappedExtent("xmax"),
        mappedExtent("ymax"))
    }

    def toTileLayout: TileLayout = {
      val mappedLayout = m.mapValues(x => x.asInstanceOf[Int])

      TileLayout(
        mappedLayout("layoutCols"),
        mappedLayout("layoutRows"),
        mappedLayout("tileCols"),
        mappedLayout("tileRows"))
    }
  }
}
