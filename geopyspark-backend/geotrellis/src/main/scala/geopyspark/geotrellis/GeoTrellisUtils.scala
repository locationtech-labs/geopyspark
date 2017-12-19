package geopyspark.geotrellis

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.mapalgebra.focal._
import geotrellis.raster.render._
import geotrellis.raster.resample.ResampleMethod
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import spray.json._
import spray.json.DefaultJsonProtocol._
import org.apache.spark.rdd._

import scala.collection.JavaConverters._
import collection.JavaConversions._


object GeoTrellisUtils {
  import  Constants._

  def seqToIterable[T](seq: Seq[T]): java.util.Iterator[T] = seq.toIterator.asJava

  def convertToScalaMap(
    javaMap: java.util.Map[String, Any]
  ): (Map[String, String], Map[String, Int]) = {
    val scalaMap = javaMap.asScala

    val intMap =
      scalaMap.filterKeys(x => INTKEYS.contains(x))
        .mapValues(x => x.asInstanceOf[Int])
        .toMap

    val stringMap =
      scalaMap.filterKeys(x => STRINGKEYS.contains(x))
        .mapValues(x => x.asInstanceOf[String])
        .toMap

    (stringMap, intMap)
  }

  def getReprojectOptions(resampleMethod: ResampleMethod): Reproject.Options = {
    import Reproject.Options

    Options(geotrellis.raster.reproject.Reproject.Options(method=resampleMethod))
  }

  def getNeighborhood(
    neighborhood: String,
    param1: Double,
    param2: Double,
    param3: Double
  ): Neighborhood =
    neighborhood match {
      case ANNULUS => Annulus(param1, param2)
      case NESW => Nesw(param1.toInt)
      case SQUARE => Square(param1.toInt)
      case WEDGE => Wedge(param1, param2, param3)
      case CIRCLE => Circle(param1)
    }

  def getOperation(
    operation: String,
    neighborhood: Neighborhood,
    cellSize: CellSize,
    param1: Double
  ): ((Tile, Option[GridBounds]) => Tile) = {
    val target = TargetCell.All

    operation match {
      case SUM => { (tile, bounds) => Sum(tile, neighborhood, bounds, target) }
      case MIN => { (tile, bounds) => Min(tile, neighborhood, bounds, target) }
      case MAX => { (tile, bounds) => Max(tile, neighborhood, bounds, target) }
      case MEAN => { (tile, bounds) => Mean(tile, neighborhood, bounds, target) }
      case MEDIAN => { (tile, bounds) => Median(tile, neighborhood, bounds, target) }
      case MODE => { (tile, bounds) => Mode(tile, neighborhood, bounds, target) }
      case STANDARDDEVIATION => { (tile, bounds) => StandardDeviation(tile, neighborhood, bounds, target) }
      case ASPECT => { (tile, bounds) => Aspect(tile, neighborhood, bounds, cellSize, target) }
      case SLOPE => { (tile, bounds) => Slope(tile, neighborhood, bounds, cellSize, param1, target) }
    }
  }

  def getBoundary(boundaryType: String): ClassBoundaryType =
    boundaryType match {
      case GREATERTHAN => GreaterThan
      case GREATERTHANOREQUALTO => GreaterThanOrEqualTo
      case LESSTHAN => LessThan
      case LESSTHANOREQUALTO => LessThanOrEqualTo
      case EXACT => Exact
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
