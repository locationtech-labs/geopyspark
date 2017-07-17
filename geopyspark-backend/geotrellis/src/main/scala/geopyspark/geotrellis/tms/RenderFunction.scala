package geopyspark.geotrellis.tms

import geotrellis.raster._
import geotrellis.raster.render._

trait TileRender {
  def requiresEncoding(): Boolean
  def render(tiles: MultibandTile): Array[Byte] = ???
  def renderEncoded(buffer: Array[Byte]): Array[Byte] = ???
}

class RenderSinglebandFromCM(cm: ColorMap) extends TileRender {
  def requiresEncoding() = false
  override def render(tile: MultibandTile) = tile.band(0).renderPng(cm).bytes
}
object RenderSinglebandFromCM {
  def apply(cm: ColorMap) = new RenderSinglebandFromCM(cm)
}

trait TileCompositer {
  def requiresEncoding(): Boolean
  def composite(tiles: Array[MultibandTile]): Array[Byte] = ???
  def compositeEncoded(buffers: Array[Array[Byte]]): Array[Byte] = ???
}
