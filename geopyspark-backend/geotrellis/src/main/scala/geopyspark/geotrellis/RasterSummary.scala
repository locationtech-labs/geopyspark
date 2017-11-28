package geopyspark.geotrellis

import geotrellis.proj4.CRS
import geotrellis.raster.{CellSize, CellType, MultibandTile}
import geotrellis.spark.tiling.{LayoutDefinition, TilerKeyMethods, ZoomedLayoutScheme}
import geotrellis.spark._
import geotrellis.util._
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

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
    val (ld, zoom) = layoutType.layoutDefinitionWithZoom(crs, extent, cellSize)
    val dataBounds: Bounds[K] = bounds.setSpatialBounds(ld.mapTransform(extent))
    TileLayerMetadata[K](cellType, ld, extent, crs, dataBounds) -> zoom
  }
}

object RasterSummary {
  /** Collect [[RasterSummary]] from unstructred rasters, grouped by CRS */
  def collect[K, K2](rdd: RDD[(K, MultibandTile)])
   (implicit
    ev: ClassTag[K],
    ev0: K => TilerKeyMethods[K, K2],
    ev1: GetComponent[K, ProjectedExtent],
    ev2: SpatialComponent[K2],
    ev3: Boundable[K2]
   ): Seq[RasterSummary[K2]] = {
    rdd
      .map { case (key, grid) =>
        val ProjectedExtent(extent, crs) = key.getComponent[ProjectedExtent]
        // Bounds are return to set the non-spatial dimensions of the KeyBounds;
        // the spatial KeyBounds are set outside this call.
        val boundsKey = key.translate(SpatialKey(0,0))
        val cellSize = CellSize(extent, grid.cols, grid.rows)
        (crs -> RasterSummary(crs, grid.cellType, cellSize, extent, KeyBounds(boundsKey, boundsKey), 1))
      }
      .reduceByKey { _ combine _ }
      .values
      .collect
      .toSeq
  }
}
