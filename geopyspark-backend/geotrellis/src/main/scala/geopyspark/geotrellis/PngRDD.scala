package geopyspark.geotrellis

import org.apache.spark.rdd.RDD

import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.render._

import scala.reflect._

object Coloring {
  def getNamedRamp(name: String): ColorRamp = {
    name match {
      case "hot" => ColorRamps.HeatmapDarkRedToYellowWhite
      case "coolwarm" => ColorRamps.BlueToRed
      case "magma" => ColorRamps.Magma
      case "inferno" => ColorRamps.Inferno
      case "plasma" => ColorRamps.Plasma
      case "viridis" => ColorRamps.Viridis

      case "BlueToOrange" => ColorRamps.BlueToOrange
      case "LightYellowToOrange" => ColorRamps.LightYellowToOrange
      case "BlueToRed" => ColorRamps.BlueToRed
      case "GreenToRedOrange" => ColorRamps.GreenToRedOrange
      case "LightToDarkSunset" => ColorRamps.LightToDarkSunset
      case "LightToDarkGreen" => ColorRamps.LightToDarkGreen
      case "HeatmapYellowToRed" => ColorRamps.HeatmapYellowToRed
      case "HeatmapBlueToYellowToRedSpectrum" => ColorRamps.HeatmapBlueToYellowToRedSpectrum
      case "HeatmapDarkRedToYellowWhite" => ColorRamps.HeatmapDarkRedToYellowWhite
      case "HeatmapLightPurpleToDarkPurpleToWhite" => ColorRamps.HeatmapLightPurpleToDarkPurpleToWhite
      case "ClassificationBoldLandUse" => ColorRamps.ClassificationBoldLandUse
      case "ClassificationMutedTerrain" => ColorRamps.ClassificationMutedTerrain
      case "Magma" => ColorRamps.Magma
      case "Inferno" => ColorRamps.Inferno
      case "Plasma" => ColorRamps.Plasma
      case "Viridis" => ColorRamps.Viridis
    }
  }

  def makeColorMap(breaks: Array[Int], name: String): ColorMap = ColorMap(breaks, getNamedRamp(name))
  def makeColorMap(breaks: Array[Double], name: String): ColorMap = ColorMap(breaks, getNamedRamp(name))

  def makeColorMap(hist: Histogram[Int], name: String): ColorMap = ColorMap.fromQuantileBreaks(hist, getNamedRamp(name))
  def makeColorMap(hist: Histogram[Double], name: String)(implicit dummy: DummyImplicit): ColorMap = ColorMap.fromQuantileBreaks(hist, getNamedRamp(name))

}

abstract class PngRDD[K: SpatialComponent :ClassTag] {
  def rdd: RDD[(K, Png)]
}

object PngRDD {
  def asSingleband(tiled: SpatialTiledRasterRDD, rampName: String): SpatialPngRDD = {
    val mapped = tiled.rdd.map{ case (key, mbtile) => (key, mbtile.band(0).renderPng(Coloring.getNamedRamp(rampName))) }
    new SpatialPngRDD(mapped.asInstanceOf[RDD[(tiled.keyType, Png)]])
  }

  def asSingleband(tiled: TemporalTiledRasterRDD, rampName: String): TemporalPngRDD = {
    val mapped = tiled.rdd.map{ case (key, mbtile) => (key, mbtile.band(0).renderPng(Coloring.getNamedRamp(rampName))) }
    new TemporalPngRDD(mapped.asInstanceOf[RDD[(tiled.keyType, Png)]])
  }

}

class SpatialPngRDD(val rdd: RDD[(SpatialKey, Png)]) extends PngRDD[SpatialKey] {
  def lookup(col: Int, row: Int): Array[Array[Byte]] =
    rdd.lookup(SpatialKey(col, row)).map(_.bytes).toArray
}

class TemporalPngRDD(val rdd: RDD[(SpaceTimeKey, Png)]) extends PngRDD[SpaceTimeKey] {
  def lookup(col: Int, row: Int, instant: Long): Array[Array[Byte]] =
    rdd.lookup(SpaceTimeKey(col, row, instant)).map(_.bytes).toArray
}

