package geopyspark.geotrellis

import geotrellis.raster.histogram._
import geotrellis.raster.io._
import spray.json._

object Json {

  def writeHistogram(hist: Histogram[_]): String = hist match {
    case h: FastMapHistogram =>
      h.asInstanceOf[Histogram[Int]].toJson.compactPrint
    case h: StreamingHistogram =>
      h.asInstanceOf[Histogram[Double]].toJson.compactPrint
    case _ =>
      throw new IllegalArgumentException(s"Unable to write $hist as JSON.")
  }

  def readHistogram(hist: String): Histogram[_] = {
    val json = hist.parseJson
    json match {
      case js: JsObject =>
        json.convertTo[Histogram[Double]]
      case js: JsArray =>
        json.convertTo[Histogram[Int]]
    }
  }
}
