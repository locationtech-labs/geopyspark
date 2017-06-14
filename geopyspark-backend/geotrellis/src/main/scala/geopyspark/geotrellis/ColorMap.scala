package geopyspark.geotrellis

import geotrellis.raster.render

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object ColorMap {

  def stringToBoundaryType(s: String) = s.toLowerCase match {
    case "greaterthan" => render.GreaterThan
    case "greaterthanorequalto" => render.GreaterThanOrEqualTo
    case "lessthan" => render.LessThan
    case "lessthanorequalto" => render.LessThanOrEqualTo
    case "exact" => render.Exact
  }

  def importInt(x: Any): Int = {
    if (x.isInstanceOf[Int])
      x.asInstanceOf[Int]
    else {
      if (x.isInstanceOf[Long])
        x.asInstanceOf[Long].toInt
      else
        throw new IllegalArgumentException("Expected integral numerical argument")
    }
  }

  def fromMap(breaks: java.util.Map[Any, Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = render.ColorMap.Options(stringToBoundaryType(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(render.ColorMap(breaks.toMap.map{ case (k, v) => (importInt(k), importInt(v)) }, opts))
  }

  def fromMapDouble(breaks: java.util.Map[Double, Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = render.ColorMap.Options(stringToBoundaryType(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(render.ColorMap(breaks.toMap.mapValues(importInt(_)), opts))
  }

  def fromBreaks(breaks: java.util.ArrayList[Any], colors: java.util.ArrayList[Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = render.ColorMap.Options(stringToBoundaryType(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(render.ColorMap(breaks.toVector.map(importInt(_)), render.ColorRamp(colors.map(importInt(_))), opts))
  }

  def fromBreaksDouble(breaks: java.util.ArrayList[Double], colors: java.util.ArrayList[Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = render.ColorMap.Options(stringToBoundaryType(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(render.ColorMap(breaks.toVector, render.ColorRamp(colors.map(importInt(_))), opts))
  }

}

case class ColorMap(cmap: render.ColorMap)
