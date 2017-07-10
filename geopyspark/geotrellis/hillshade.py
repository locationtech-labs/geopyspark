from geopyspark.geotrellis.layer import TiledRasterLayer


def hillshade(tiled_raster_layer, band=0, azimuth=315.0, altitude=45.0, z_factor=1.0):
    """Computes Hillshade (shaded relief) from a raster.

    The resulting raster will be a shaded relief map (a hill shading) based
    on the sun altitude, azimuth, and the z factor. The z factor is a
    conversion factor from map units to elevation units.

    Returns a raster of ShortConstantNoDataCellType.

    For descriptions of parameters, please see Esri Desktop's
    `description <http://goo.gl/DtVDQ>`_ of Hillshade.

    Args:
        band (int) [default = 0]: The band of the raster to base the
            hillshade calculation on.
        azimuth (float) [default = 315]
        altitude (float) [default = 45]
        z_factor (float) [default = 1.0]
    """

    srdd = tiled_raster_layer.srdd.hillshade(tiled_raster_layer.pysc._jsc.sc(), azimuth,
                                             altitude, z_factor, band)

    return TiledRasterLayer(tiled_raster_layer.pysc, tiled_raster_layer.layer_type, srdd)
