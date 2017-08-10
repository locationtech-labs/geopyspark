from geopyspark.geotrellis.layer import TiledRasterLayer


__all__ = ['hillshade']


def hillshade(tiled_raster_layer, band=0, azimuth=315.0, altitude=45.0, z_factor=1.0):
    """Computes Hillshade (shaded relief) from a raster.

    The resulting raster will be a shaded relief map (a hill shading) based
    on the sun altitude, azimuth, and the z factor. The z factor is a
    conversion factor from map units to elevation units.

    Returns a raster of ShortConstantNoDataCellType.

    For descriptions of parameters, please see Esri Desktop's
    `description <http://goo.gl/DtVDQ>`_ of Hillshade.

    Args:
        tiled_raster_layer (:class:`~geopyspark.geotrellis.layer.TiledRasterLayer`): The base layer
            that contains the rasters used to compute the hillshade.
        band (int, optional): The band of the raster to base the hillshade calculation on. Default is 0.
        azimuth (float, optional): The azimuth angle of the source of light. Default value is 315.0.
        altitude (float, optional): The angle of the altitude of the light above the horizon. Default is
            45.0.
        z_factor (float, optional): How many x and y units in a single z unit. Default value is 1.0.

    Returns:
        :class:`~geopyspark.geotrellis.layer.TiledRasterLayer`
    """

    srdd = tiled_raster_layer.srdd.hillshade(tiled_raster_layer.pysc._jsc.sc(), azimuth,
                                             altitude, z_factor, band)

    return TiledRasterLayer(tiled_raster_layer.layer_type, srdd)
