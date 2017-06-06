package geopyspark.geotrellis.tms

trait TileRender {
  def render(cells: Array[Int], cols: Int, rows: Int): Array[Byte]
}
