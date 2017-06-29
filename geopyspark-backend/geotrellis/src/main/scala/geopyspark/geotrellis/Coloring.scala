package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render._

object Coloring {
  def getColorRamp(name: String): ColorRamp = {
    name match {
      case "Hot" => ColorRamps.HeatmapDarkRedToYellowWhite
      case "CoolWarm" => ColorRamps.BlueToRed
      case "Magma" => ColorRamps.Magma
      case "Inferno" => ColorRamps.Inferno
      case "Plasma" => ColorRamps.Plasma
      case "Viridis" => ColorRamps.Viridis

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
    }
  }

  def getColorRamp(colors: Array[Int]): ColorRamp = {
    ColorRamp(colors.toVector)
  }

  def makeColorMap(breaks: Array[Int], ramp: ColorRamp): ColorMap =
    ColorMap(breaks, ramp)
  def makeColorMap(breaks: Array[Double], ramp: ColorRamp): ColorMap =
    ColorMap(breaks, ramp)
}

object ColorRampUtils {
  def get(name: String): Array[Int] =
    Coloring.getColorRamp(name).colors.toArray

  def get(name: String, numColors: Int): Array[Int] =
    Coloring.getColorRamp(name).stops(numColors).colors.toArray

  def getHex(name: String): Array[String] =
    get(name).map(x => s"#${x.toHexString.toUpperCase}")

  def getHex(name: String, numColors: Int): Array[String] =
    get(name, numColors).map(x => s"#${x.toHexString.toUpperCase}")

}
