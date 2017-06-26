import shapely.wkb
from geopyspark.geotrellis.layer import TiledRasterLayer


def cost_distance(tiled_raster_layer, geometries, max_distance):
    """Performs cost distance of a TileLayer.

    Args:
        geometries (list):
            A list of shapely geometries to be used as a starting point.

            Note:
                All geometries must be in the same CRS as the TileLayer.
        max_distance (int, float): The maximum cost that a path may reach before the operation.
            stops. This value can be an ``int`` or ``float``.

    Returns:
        :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
    """

    wkbs = [shapely.wkb.dumps(g) for g in geometries]
    srdd = tiled_raster_layer.srdd.costDistance(tiled_raster_layer.geopysc.sc, wkbs,
                                                float(max_distance))

    return TiledRasterLayer(tiled_raster_layer.geopysc, tiled_raster_layer.rdd_type, srdd)
