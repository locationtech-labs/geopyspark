from geopyspark import get_spark_context
from geopyspark.geotrellis import LayerType
from geopyspark.geotrellis.layer import RasterLayer, TiledRasterLayer


__all__ = ['union']


def union(*layers):
    """Unions togther two or more ``RasterLayer``\s or ``TiledRasterLayer``\s.

    All layers must have the same ``layer_type``. If the layers are ``TiledRasterLayer``\s,
    then all of the layers must also have the same :class:`~geopyspark.geotrellis.TileLayout`
    and ``CRS``.

    Note:
        If the layers to be unioned share one or more keys, then the resulting layer will contain
        duplicates of that key. One copy for each instance of the key.

    Args:
        layers (*:class:`~geopyspark.RasterLayer` or *:class:`~geopyspark.TiledRasterLayer`): An
            arbitrary number (that is more than one) of layers to be unioned together.

    Returns:
        :class:`~geopyspark.RasterLayer` or :class:`~geopyspark.TiledRasterLayer`
    """

    if len(layers) == 1:
        raise ValueError("Union can only performed on 2 or more layers")

    base_layer = layers[0]
    base_layer_type = base_layer.layer_type
    base_tile_layout = None
    base_crs = None

    if not all(isinstance(x, type(base_layer)) for x in layers[1:]):
        raise TypeError("All of the layers to be unioned must be the same type")

    def check_layer(layer):
        type_check = layer.layer_type == base_layer_type

        if base_tile_layout:
            layout_check = layer.layer_metadata.layout_definition.tileLayout == base_tile_layout
            crs_check = layer.layer_metadata.crs == base_crs

            if layout_check and type_check and crs_check:
                return True
            else:
                return False
        else:
            return type_check


    if isinstance(base_layer, RasterLayer):
        if not all([check_layer(x) for x in layers[1:]]):
            raise ValueError("The layers to be unioned must have the same layer_type")

        pysc = get_spark_context()

        if base_layer_type == LayerType.SPATIAL:
            result = pysc._gateway.jvm.geopyspark.geotrellis.ProjectedRasterLayer.unionLayers(pysc._jsc.sc(),
                                                                                              [x.srdd for x in layers])
        else:
            result = pysc._gateway.jvm.geopyspark.geotrellis.TemporalRasterLayer.unionLayers(pysc._jsc.sc(),
                                                                                             [x.srdd for x in layers])

        return RasterLayer(base_layer_type, result)

    else:
        base_tile_layout = base_layer.layer_metadata.layout_definition.tileLayout
        base_crs = base_layer.layer_metadata.crs

        if not all([check_layer(x) for x in layers[1:]]):
            raise ValueError("The layers to be unioned must have the same TileLayout, CRS, and layer_type")

        pysc = get_spark_context()

        if base_layer_type == LayerType.SPATIAL:
            result = pysc._gateway.jvm.geopyspark.geotrellis.SpatialTiledRasterLayer.unionLayers(pysc._jsc.sc(),
                                                                                                 [x.srdd for x in layers])
        else:
            result = pysc._gateway.jvm.geopyspark.geotrellis.TemporalTiledRasterLayer.unionLayers(pysc._jsc.sc(),
                                                                                                  [x.srdd for x in layers])
        return TiledRasterLayer(base_layer_type, result)
