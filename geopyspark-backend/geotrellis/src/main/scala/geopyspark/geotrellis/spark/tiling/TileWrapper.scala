package geopyspark.geotrellis.spark.tiling

import geopyspark.geotrellis._

import geotrellis.raster.resample._
import geotrellis.spark.tiling._

import org.apache.spark._

import scala.collection.JavaConverters._
import collection.JavaConversions._
import java.util.Map


object TilerOptions {
  def default = Tiler.Options.DEFAULT

  def getResampleMethod(resampleMethod: String): ResampleMethod =
    resampleMethod match {
      case "NearestNeighbor" => NearestNeighbor
      case "Bilinear" => Bilinear
      case "CubicConvolution" => CubicConvolution
      case "CubicSpline" => CubicSpline
      case "Lanczos" => Lanczos
      case "Average" => Average
      case "Mode" => Mode
      case "Median" => Median
      case "Max" => Max
      case "Min" => Min
      case _ => throw new Exception(s"$resampleMethod, is not a known sampling method")
    }

  def setValues(reMethod: String): Tiler.Options = {
    val resampleMethod = getResampleMethod(reMethod)

    Tiler.Options(
      resampleMethod = resampleMethod,
      partitioner = default.partitioner)
  }
}
