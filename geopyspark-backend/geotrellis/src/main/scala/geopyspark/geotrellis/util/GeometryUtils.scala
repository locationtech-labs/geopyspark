package geopyspark.geotrellis.util

import geotrellis.vector.Geometry
import geotrellis.vector.io.wkb.WKB

object GeometryUtil {

  def wkbToScalaGeometry(wkb: Array[Byte]): Geometry = WKB.read(wkb)

}
