package geopyspark.geotrellis.tms

import geotrellis.raster._
import geotrellis.raster.render._

trait TileRender {
  def render(tile: Tile): Array[Byte]
}

class RenderFromCM(cm: ColorMap) extends TileRender {
  def render(tile: Tile) = tile.renderPng(cm).bytes
}
