import shapely.wkb
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer


def euclidean_distance(pysc, geometry, source_crs, zoom, cellType='float64'):
    """Calculates the Euclidean distance of a Shapely geometry.

    Args:
        pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
        geometry (shapely.geometry): The input geometry to compute the Euclidean distance
            for.
        source_crs (str or int): The CRS of the input geometry.
        zoom (int): The zoom level of the output raster.

    Note:
        This function may run very slowly for polygonal inputs if they cover many cells of
        the output raster.

    Returns:
        :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
    """

    if isinstance(source_crs, int):
        source_crs = str(source_crs)

    srdd = pysc._gateway.jvm.geopyspark.geotrellis.SpatialTiledRasterRDD.euclideanDistance(pysc._jsc.sc(),
                                                                                           shapely.wkb.dumps(geometry),
                                                                                           source_crs,
                                                                                           cellType,
                                                                                           zoom)
    return TiledRasterLayer(pysc, LayerType.SPATIAL, srdd)
