package geopyspark.geotrellis.io

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.vector._


class TileLayerMetadataWrapper[K](val md: TileLayerMetadata[K]) {

  var TileLayerMetadata(cellType, layout, extent, crs, bounds) = md

  def mutateCellType(ct: String): Unit = {
    cellType = CellType.fromString(ct)
  }

  def mutateExtent(xmin: Double, ymin: Double, xmax: Double, ymax: Double): Unit = {
    extent = Extent(xmin, ymin, xmax, ymax)
  }

  def mutateCrsProj4(proj: String): Unit = {
    crs = CRS.fromString(proj)
  }

  def mutateCrsWkt(wkt: String): Unit = {
    crs = CRS.fromWKT(wkt)
  }

  def mutateCrsName(name: String): Unit = {
    crs = CRS.fromName(name)
  }

}
