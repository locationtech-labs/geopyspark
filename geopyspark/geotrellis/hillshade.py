from geopyspark.geotrellis.layer import TiledRasterLayer


__all__ = ['hillshade']


def hillshade(tiled_raster_layer, zfactor_calculator, band=0, azimuth=315.0, altitude=45.0):
    """Computes Hillshade (shaded relief) from a raster.

    The resulting raster will be a shaded relief map (a hill shading) based
    on the sun altitude, azimuth, and the ``zfactor``. The ``zfactor`` is a
    conversion factor from map units to elevation units.

    The ``hillshade``` operation will be carried out in a ``SQUARE`` neighborhood with with an
    ``extent`` of 1.  The ``zfactor`` will be derived from the ``zfactor_calculator``
    for each ``Tile`` in the Layer. The resulting Layer will have a ``cell_type``
    of ``INT16`` regardless of the input Layer's ``cell_type``; as well as
    have a single band, that represents the calculated ``hillshade``.

    Returns a raster of ShortConstantNoDataCellType.

    For descriptions of parameters, please see Esri Desktop's
    `description <http://goo.gl/DtVDQ>`_ of Hillshade.

    Args:
        tiled_raster_layer (:class:`~geopyspark.geotrellis.layer.TiledRasterLayer`): The base layer
            that contains the rasters used to compute the hillshade.
        zfactor_calculator (py4j.JavaObject): A ``JavaObject`` that represents the
            Scala ``ZFactorCalculator`` class. This can be created using either the
            :meth:`~geopyspark.geotrellis.zfactor_lat_lng_calculator` or the
            :meth:`~geopyspark.geotrellis.zfactor_calculator` methods.
        band (int, optional): The band of the raster to base the hillshade calculation on. Default is 0.
        azimuth (float, optional): The azimuth angle of the source of light. Default value is 315.0.
        altitude (float, optional): The angle of the altitude of the light above the horizon. Default is
            45.0.

    Returns:
        :class:`~geopyspark.geotrellis.layer.TiledRasterLayer`
    """

    srdd = tiled_raster_layer.srdd.hillshade(azimuth, altitude, zfactor_calculator, band)

    return TiledRasterLayer(tiled_raster_layer.layer_type, srdd)
