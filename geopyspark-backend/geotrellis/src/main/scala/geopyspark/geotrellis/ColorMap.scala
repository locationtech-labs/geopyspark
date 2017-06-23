package geopyspark.geotrellis

import geotrellis.raster.render._
import geotrellis.raster.histogram._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object ColorMapUtils {

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
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.toMap.map{ case (k, v) => (importInt(k), importInt(v)) }, opts)
  }

  def fromMapDouble(breaks: java.util.Map[Double, Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.toMap.mapValues(importInt(_)), opts)
  }

  def fromBreaks(breaks: java.util.ArrayList[Any], colors: java.util.ArrayList[Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.toVector.map(importInt(_)), ColorRamp(colors.map(importInt(_))), opts)
  }

  def fromBreaksDouble(breaks: java.util.ArrayList[Double], colors: java.util.ArrayList[Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    ColorMap(breaks.toVector, ColorRamp(colors.map(importInt(_))), opts)
  }

  def fromHistogram(hist: IntHistogram, colors: java.util.ArrayList[Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    val ramp  = ColorRamp(colors.map(importInt(_)))
    ColorMap.fromQuantileBreaks(hist, ramp, opts)
  }

  def fromHistogram(hist: StreamingHistogram, colors: java.util.ArrayList[Any], noDataColor: Any, fallbackColor: Any, boundaryType: String) = {
    val opts = ColorMap.Options(GeoTrellisUtils.getBoundary(boundaryType), importInt(noDataColor), importInt(fallbackColor))
    val ramp  = ColorRamp(colors.map(importInt(_)))
    ColorMap.fromQuantileBreaks(hist, ramp, opts)
  }
}
