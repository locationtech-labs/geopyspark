'''
A module to provide facilites for converting between layout keys and spatial objects.  These facilities aim to bridge between the various layouts, ``SpatialKey``\s, and geometry.
'''

from math import ceil

import geopyspark as gps
from . import Extent

__all__ = ["KeyTransform"]

"""The WebMercator (EPSG=3857) world extent"""
WEB_MERCATOR = Extent(-20037508.342789244, -20037508.342789244, 20037508.342789244, 20037508.342789244)

"""The LatLng (EPSG=4326) world extent"""
LATLNG = Extent(-180.0, -89.99999, 179.99999, 89.99999)

class KeyTransform(object):
    """Provides functions to move from keys to geometry and vice-versa.

    Tile Layers have an underlying RDD which is keyed by either :class:`~geopyspark.geotrellis.SpatialKey` or
    :class:`~geopyspark.geotrellis.SpaceTimeKey`.  Each key represents a region in space, depending on a choice of layout.
    In order to enable the conversion of keys to regions, and of geometry to keys, the ``KeyTransform`` class is provided.
    This class is constructed with a layout, which is either ``GlobalLayout``, ``LocalLayout``, or a ``LayoutDefinition``.
    Global layouts use power-of-two pyramids over the world extent, while local layouts operate over a defined extent and
    cellsize.

    NOTE: LocalLayouts will encompass the requested extent, but the final layout may include ``SpatialKey``s which only
    partially cover the requested extent.  The upper-left corner of the resulting layout will match the requested extent,
    but the right and bottom edges may be beyond the boundaries of the requested extent.

    NOTE: GlobalLayouts require pyproj to be installed.

    Args:
        layout(:class:`~geopyspark.geotrellis.GlobalLayout` or :class:`~geopyspark.geotrellis.LocalLayout`
            or :class:`~geopyspark.geotrellis.LayoutDefinition`): a definition of the layout scheme defining the key structure.
        crs (str or int): Used only when `layout` is :class:`~geopyspark.geotrellis.GlobalLayout`.  Target CRS of reprojection.
            Either EPSG code, well-known name, or a PROJ.4 string
        extent (:class:`~geopyspark.geotrellis.Extent`): Used only for ``LocalLayout``s.  The area of interest.
        cellsize (tup of (float, float)): Used only for ``LocalLayout``s.  The (width, height) in extent units of a pixel.
            Cannot be specified simultaneously with ``dimensions``.
        dimensions (tup of (int, int)): Used only for ``LocalLayout``s.  The number of (columns, rows) of pixels over the
            entire extent.  Cannot be specified simultaneously with ``cellsize``.
    """

    def __init__(self, layout, crs=None, extent=None, cellsize=None, dimensions=None):
        self.__jvm = gps.get_spark_context()._gateway.jvm

        if isinstance(layout, gps.LocalLayout):
            if not extent:
                raise ValueError("Must specify an extent when using LocalLayout")

            if dimensions and not cellsize:
                cellsize = ((extent.xmax - extent.xmin)/dimensions[0], (extent.ymax - extent.ymin)/dimensions[1])
                dimensions = None

            if cellsize and not dimensions:
                tilewidth = layout.tile_cols * cellsize[0]
                tileheight = layout.tile_rows * cellsize[1]
                rows = ceil((extent.xmax - extent.xmin) / tilewidth)
                cols = ceil((extent.ymax - extent.ymin) / tileheight)
                extent = gps.Extent(extent.xmin, extent.ymax - rows * tileheight, extent.xmin + cols * tilewidth, extent.ymax)
                tl = gps.TileLayout(cols, rows, layout.tile_cols, layout.tile_rows)
            else:
                raise ValueError("For LocalLayout, must specify exactly one: cellsize or dimension")
        elif isinstance(layout, gps.GlobalLayout):
            try:
                from pyproj import Proj, transform
            except:
                raise ImportError('pyproj is required for GlobalLayout')

            if not layout.zoom:
                raise ValueError("Must specify a zoom level when using GlobalLayout")

            if not crs:
                raise ValueError("Must specify a crs when using GlobalLayout")

            if isinstance(crs, int):
                crs = "{}".format(crs)

            gtcrs = self.__jvm.geopyspark.geotrellis.TileLayer.getCRS(crs).get()

            if gtcrs.epsgCode().isDefined() and gtcrs.epsgCode().get() == 3857:
                extent = WEB_MERCATOR
            elif gtcrs.epsgCode().isDefined() and gtcrs.epsgCode().get() == 4326:
                extent = LATLNG
            else:
                llex = LATLNG
                proj4str = gtcrs.toProj4String()
                target = Proj(proj4str)
                xmin, ymin = target(llex.xmin, llex.ymin)
                xmax, ymax = target(llex.xmax, llex.ymax)
                extent = gps.Extent(xmin, ymin, xmax, ymax)

            layout_rows_cols = int(pow(2, layout.zoom))
            tl = gps.TileLayout(layout_rows_cols, layout_rows_cols, layout.tile_size, layout.tile_size)
        elif isinstance(layout, gps.LayoutDefinition):
            extent = layout.extent
            tl = layout.tileLayout

        ex = self.__jvm.geotrellis.vector.Extent(float(extent.xmin), float(extent.ymin), float(extent.xmax), float(extent.ymax))
        tilelayout = self.__jvm.geotrellis.raster.TileLayout(int(tl[0]), int(tl[1]), int(tl[2]), int(tl[3]))
        self.layout = gps.LayoutDefinition(extent, tl)
        self.__layout = self.__jvm.geotrellis.spark.tiling.LayoutDefinition(ex, tilelayout)

    def key_to_extent(self, key, *args):
        """Returns the Extent corresponding to a given key.

        Args:
            key (:class:`~geopyspark.geotrellis.SpatialKey` or :class:`~geopyspark.geotrellis.SpaceTimeKey` or int): The
                key to find the extent for.  If of type int, then this parameter is the column of the key, and the call
                must provide a single additional int value in the args parameter to serve as the row of the key.

        Returns:
            :class:`~geopyspark.geotrellis.Extent`
        """
        if isinstance(key, (gps.SpatialKey, gps.SpaceTimeKey)):
            skey = self.__jvm.geotrellis.spark.SpatialKey(key.col, key.row)
        elif isinstance(key, tuple):
            skey = self.__jvm.geotrellis.spark.SpatialKey(key[0], key[1])
        elif isinstance(key, int) and len(args) == 1 and isinstance(args[0], int):
            skey = self.__jvm.geotrellis.spark.SpatialKey(key, args[0])
        else:
            raise ValueError("Please supply either gps.SpatialKey, gps.SpaceTimeKey, (int, int), or two ints")
        ex = self.__layout.mapTransform().apply(skey)
        return gps.Extent(ex.xmin(), ex.ymin(), ex.xmax(), ex.ymax())

    def extent_to_keys(self, extent):
        """Returns the keys in the layout intersecting/covered by a given extent.

        Args:
            extent (:class:`~geopyspark.geotrellis.Extent`): The extent to find the matching keys for.

        Returns:
            [:class:`~geopyspark.geotrellis.SpatialKey`]
        """
        ex = self.__jvm.geotrellis.vector.Extent(float(extent.xmin), float(extent.ymin), float(extent.xmax), float(extent.ymax))
        gridbnd = self.__layout.mapTransform().apply(ex)
        cmin = gridbnd.colMin()
        cmax = gridbnd.colMax()
        rmin = gridbnd.rowMin()
        rmax = gridbnd.rowMax()
        return (gps.SpatialKey(c, r) for c in range(cmin, cmax + 1) for r in range(rmin, rmax + 1))

    def geometry_to_keys(self, geom):
        """Returns the keys corresponding to grid cells that intersect/are covered by a given Shapely geometry.

        Args:
            geom (:class:`~shapely.geometry.Geometry`): The geometry to find the matching keys for.

        Returns:
            [:class:`~geopyspark.geotrellis.SpatialKey`]
        """
        from shapely.wkb import dumps
        jts_geom = self.__jvm.geopyspark.geotrellis.util.GeometryUtil.wkbToScalaGeometry(dumps(geom))
        scala_key_set = self.__layout.mapTransform().keysForGeometry(jts_geom)
        key_set = self.__jvm.scala.collection.JavaConverters.setAsJavaSetConverter(scala_key_set).asJava()
        return [gps.SpatialKey(key.col(), key.row()) for key in key_set]
