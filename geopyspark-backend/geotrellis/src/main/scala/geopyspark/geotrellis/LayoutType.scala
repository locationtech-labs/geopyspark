package geopyspark.geotrellis

import geotrellis.proj4.CRS
import geotrellis.raster.CellSize
import geotrellis.spark.tiling.{FloatingLayoutScheme, LayoutDefinition, LayoutScheme, ZoomedLayoutScheme}
import geotrellis.vector.Extent

/** Strategy for selecting LayoutScheme before metadata is collected */
sealed trait LayoutType {
  def tileSize: Int
  def layoutDefinition(crs: CRS, extent: Extent, cellSize: CellSize): LayoutDefinition
}

/** @see [[geotrellis.spark.tiling.ZoomedLayoutScheme]] */
case class GlobalLayout(tileSize: Int, zoom: Integer, threshold: Double)  extends LayoutType {
  def layoutDefinition(crs: CRS, extent: Extent, cellSize: CellSize): LayoutDefinition = {
    val scheme = new ZoomedLayoutScheme(crs, tileSize, threshold)
    Option(zoom) match {
      case Some(zoom) => scheme.levelForZoom(zoom).layout
      case None => scheme.levelFor(extent, cellSize).layout
    }
  }
}

/** @see [[geotrellis.spark.tiling.FloatingLayoutScheme]] */
case class LocalLayout(tileSize: Int) extends LayoutType {
  def layoutDefinition(crs: CRS, extent: Extent, cellSize: CellSize): LayoutDefinition = {
    val scheme = new FloatingLayoutScheme(tileSize, tileSize)
    scheme.levelFor(extent, cellSize).layout
  }
}