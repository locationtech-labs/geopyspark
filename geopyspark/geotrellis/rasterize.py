import shapely.wkb
from geopyspark.geotrellis.constants import SPATIAL
from geopyspark.geotrellis.layer import TiledRasterLayer


def rasterize(geopysc, geoms, crs, zoom, fill_value, cell_type='float64', options=None, numPartitions=None):
    """Rasterizes a Shapely geometries.

    Args:
        geopysc (:class:`~geopyspark.GeoPyContext`): The ``GeoPyContext`` instance.
        geoms ([shapely.geometry]): List of shapely geometries to rasterize.
        crs (str or int): The CRS of the input geometry.
        zoom (int): The zoom level of the output raster.
        fill_value: Value to burn into pixels intersectiong geometry
        cell_type (str): The string representation of the ``CellType`` to convert to.
        options (:class:`~geopyspark.geotrellis.RasterizerOptions`): Pixel intersection options.

    Returns:
        :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
    """
    if isinstance(crs, int):
        crs = str(crs)

    wkb_geoms = [shapely.wkb.dumps(g) for g in geoms]
    srdd = geopysc._jvm.SpatialTiledRasterRDD.rasterizeGeometry(geopysc.sc, wkb_geoms, crs, zoom,
                                                                float(fill_value), cell_type,
                                                                options, numPartitions)
    return TiledRasterLayer(geopysc, SPATIAL, srdd)

