import shapely.wkb
from geopyspark import get_spark_context
from geopyspark.geotrellis.constants import LayerType, CellType
from geopyspark.geotrellis.layer import TiledRasterLayer


__all__ = ['euclidean_distance']


def euclidean_distance(geometry, source_crs, zoom, cell_type=CellType.FLOAT64):
    """Calculates the Euclidean distance of a Shapely geometry.

    Args:
        geometry (shapely.geometry): The input geometry to compute the Euclidean distance
            for.
        source_crs (str or int): The CRS of the input geometry.
        zoom (int): The zoom level of the output raster.
        cell_type (str or :class:`~geopyspark.geotrellis.constants.CellType`, optional): The data
            type of the cells for the new layer. If not specified, then ``CellType.FLOAT64`` is used.

    Note:
        This function may run very slowly for polygonal inputs if they cover many cells of
        the output raster.

    Returns:
        :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
    """

    if isinstance(source_crs, int):
        source_crs = str(source_crs)

    pysc = get_spark_context()

    srdd = pysc._gateway.jvm.geopyspark.geotrellis.SpatialTiledRasterLayer.euclideanDistance(pysc._jsc.sc(),
                                                                                             shapely.wkb.dumps(geometry),
                                                                                             source_crs,
                                                                                             CellType(cell_type).value,
                                                                                             zoom)
    return TiledRasterLayer(LayerType.SPATIAL, srdd)
