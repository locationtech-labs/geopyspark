package geopyspark.geotrellis

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType}
import geotrellis.spark.tiling.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.spark.{Boundable, Bounds, KeyBounds, SpatialComponent, TileLayerMetadata}
import geotrellis.vector.Extent

case class RasterSummary[K](
  crs: CRS,
  cellType: CellType,
  cellSize: CellSize,
  extent: Extent,
  bounds: KeyBounds[K],
  tileCount: Long
) {
  def combine(other: RasterSummary[K])(implicit ev: Boundable[K]) = {
    require(other.crs == crs, s"Can't combine LayerExtent for different CRS: $crs, ${other.crs}")
    val smallestCellSize = if (cellSize.resolution < other.cellSize.resolution) cellSize else other.cellSize
    RasterSummary(
      crs,
      cellType.union(other.cellType),
      smallestCellSize,
      extent.combine(other.extent),
      bounds.combine(other.bounds),
      tileCount + other.tileCount
    )
  }

  def toTileLayerMetadata(layoutType: LayoutType)(implicit ev: SpatialComponent[K]) = {
    val ld: LayoutDefinition = layoutType.layoutDefinition(crs, extent, cellSize)
    val dataBounds: Bounds[K] = bounds.setSpatialBounds(ld.mapTransform(extent))
    TileLayerMetadata[K](cellType, ld, extent, crs, dataBounds)
  }
}
