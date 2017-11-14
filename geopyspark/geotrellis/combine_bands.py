from geopyspark import get_spark_context
from geopyspark.geotrellis import LayerType, check_layers
from geopyspark.geotrellis.layer import RasterLayer, TiledRasterLayer


__all__ = ['combine_bands']


def combine_bands(layers):
    """Combines the bands of values that share the same key in two or more ``TiledRasterLayer``\s.

    This method will concat the bands of two or more values with the same key. For example,
    ``layer a`` has values that have 2 bands and ``layer b`` has values with 1 band. When
    ``combine_bands`` is used on both of these layers, then the resulting layer will have
    values with 3 bands, 2 from ``layer a`` and 1 from ``layer b``.

    Note:
        All layers must have the same ``layer_type``. If the layers are ``TiledRasterLayer``\s,
        then all of the layers must also have the same :class:`~geopyspark.geotrellis.TileLayout`
        and ``CRS``.

    Args:
        layers ([:class:`~geopyspark.RasterLayer`] or [:class:`~geopyspark.TiledRasterLayer`] or (:class:`~geopyspark.RasterLayer`) or (:class:`~geopyspark.TiledRasterLayer`)): A
            colection of two or more ``RasterLayer``\s or ``TiledRasterLayer``\s. **The order of the
            layers determines the order in which the bands are concatenated**. With the bands being
            ordered based on the position of their respective layer.

            For example, the first layer in ``layers`` is ``layer a`` which contains 2 bands and
            the second layer is ``layer b`` whose values have 1 band. The resulting layer will
            have values with 3 bands: the first 2 are from ``layer a`` and the third from ``layer b``.
            If the positions of ``layer a`` and ``layer b`` are reversed, then the resulting values'
            first band will be from ``layer b`` and the last 2 will be from ``layer a``.

    Returns:
        :class:`~geopyspark.RasterLayer` or :class:`~geopyspark.TiledRasterLayer`
    """

    if len(layers) == 1:
        raise ValueError("combine_bands can only be performed on 2 or more layers")

    base_layer = layers[0]
    base_layer_type = base_layer.layer_type

    check_layers(base_layer, base_layer_type, layers)

    pysc = get_spark_context()

    if isinstance(base_layer, RasterLayer):
        if base_layer_type == LayerType.SPATIAL:
            result = pysc._gateway.jvm.geopyspark.geotrellis.ProjectedRasterLayer.combineBands(pysc._jsc.sc(),
                                                                                               [x.srdd for x in layers])
        else:
            result = pysc._gateway.jvm.geopyspark.geotrellis.TemporalRasterLayer.combineBands(pysc._jsc.sc(),
                                                                                              [x.srdd for x in layers])

        return RasterLayer(base_layer_type, result)

    else:
        if base_layer_type == LayerType.SPATIAL:
            result = pysc._gateway.jvm.geopyspark.geotrellis.SpatialTiledRasterLayer.combineBands(pysc._jsc.sc(),
                                                                                                  [x.srdd for x in layers])
        else:
            result = pysc._gateway.jvm.geopyspark.geotrellis.TemporalTiledRasterLayer.combineBands(pysc._jsc.sc(),
                                                                                                   [x.srdd for x in layers])
        return TiledRasterLayer(base_layer_type, result)
