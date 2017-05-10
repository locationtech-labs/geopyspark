"""Contains data structures that represent various GeoTrellis types.

Because GeoPySpark is a python bindings library of GeoTrellis, certain GeoTrellis data types
have been brought over. It is here that they are defined.
"""
from collections import namedtuple


Extent = namedtuple("Extent", 'xmin ymin xmax ymax')
"""
The "bounding box" or geographic region of an area on Earth a raster represents.

Args:
    xmin (float): The minimum x coordinate.
    ymin (float): The minimum y coordinate.
    xmax (float): The maximum x coordinate.
    ymax (float): The maximum y coordinate.

Returns:
    :obj:`~geopyspark.geotrellis.data_structures.Extent`
"""

TileLayout = namedtuple("TileLayout", 'layoutCols layoutRows tileCols tileRows')
"""
Describes the grid in which the rasters within a RDD should be laid out.

Args:
    layoutCols (int): The number of columns of rasters that runs east to west.
    layoutRows (int): The number of rows of rasters that runs north to south.
    tileCols (int): The number of columns of pixels in each raster that runs east to west.
    tileRows (int): The number of rows of pixels in each raster that runs north to south.
Returns:
    :obj:`~geopyspark.geotrellis.data_structures.TileLayout`
"""

LayoutDefinition = namedtuple("LayoutDefinition", 'extent tileLayout')
"""
Describes the layout of the rasters within a RDD and how they are projected.

Args:
    extent (:obj:`~geopyspark.geotrellis.data_structres.Extent`): The ``Extent`` of the layout.
    tileLayout (:obj:`~geopyspark.geotrellis.data_structres.TileLayout`): The ``TileLayout`` of
        how the rasters within the RDD.

Returns:
    :obj:`~geopyspark.geotrellis.data_structures.LayoutDefinition`
"""

Bounds = namedtuple("Bounds", 'minKey maxKey')
"""
Represents the grid that covers the area of the rasters in a RDD on a grid.

Args:
    minKey (:ref:`spatial-key` or :ref:`space-time-key`): The smallest ``SpatialKey`` or
        ``SpaceTimeKey``.
    maxKey (:ref:`spatial-key` or :ref:`space-time-key`): The largest ``SpatialKey`` or
        ``SpaceTimeKey``.

Returns:
    :obj:`~geopyspark.geotrellis.data_structures.Bounds`
"""


class Metadata(object):
    """Information of the values within ``RasterRDD`` or ``TiledRasterRDD``.
    This data pertains to the layout and other attributes of the data within the classes.

    Args:
        bounds (:obj:`~geopyspark.geotrellis.data_structres.Bounds`): The ``Bounds`` of the
            values in the class.
        crs (str or int): The ``CRS`` of the data. Can either be the EPSG code, well-known name, or
            a PROJ.4 projection string.
        cell_type (str): The data type of the cells of the rasters.
        extent (:obj:`~geopyspark.geotrellis.data_structures.Extent`): The ``extent`` that covers
            the all of the rasters.
        layout_definition (:obj:`~geopyspark.geotrellis.data_structures.LaoutDefinition`): The
            ``LayoutDefinition`` of all rasters.

    Attributes:
        bounds (:obj:`~geopyspark.geotrellis.data_structres.Bounds`): The ``Bounds`` of the
            values in the class.
        crs (str or int): The ``CRS`` of the data. Can either be the EPSG code, well-known name, or
            a PROJ.4 projection string.
        cell_type (str): The data type of the cells of the rasters.
        extent (:obj:`~geopyspark.geotrellis.data_structures.Extent`): The ``extent`` that covers
            the all of the rasters.
        tile_layout (:obj:`~geopyspark.geotrellis.data_structures.TileLayout`): The ``TileLayout``
            that describes how the rasters are orginized.
        layout_definition (:obj:`~geopyspark.geotrellis.data_structures.LayoutDefinition`): The
            ``LayoutDefinition`` of all rasters.
    """

    def __init__(self, bounds, crs, cell_type, extent, layout_definition):
        self.bounds = bounds
        self.crs = crs
        self.cell_type = cell_type
        self.extent = extent
        self.tile_layout = layout_definition.tileLayout
        self.layout_definition = layout_definition

    @classmethod
    def from_dict(cls, metadata_dict):
        """Creates ``Metadata`` from a dictionary.

        Args:
            metadata_dict (dict): The ``Metadata`` of a ``RasterRDD`` and ``TiledRasterRDD``
                in ``dict`` form.

        Returns:
            :class:`~geopyspark.geotrellis.data_structures.Metadata`
        """

        cls._metadata_dict = metadata_dict

        crs = metadata_dict['crs']
        cell_type = metadata_dict['cellType']

        bounds = Bounds(**metadata_dict['bounds'])
        extent = Extent(**metadata_dict['extent'])

        layout_definition = LayoutDefinition(
            Extent(**metadata_dict['layoutDefinition']['extent']),
            TileLayout(**metadata_dict['layoutDefinition']['tileLayout']))

        return cls(bounds, crs, cell_type, extent, layout_definition)

    def to_dict(self):
        """Converts this instance to a ``dict``.

        Returns:
            dict
        """

        if not hasattr(self, '_metadata_dict'):
            self._metadata_dict = {
                'bounds': self.bounds._asdict(),
                'crs': self.crs,
                'cellType': self.cell_type,
                'extent': self.extent._asdict(),
                'layoutDefinition': {
                    'extent': self.layout_definition.extent._asdict(),
                    'tileLayout': self.tile_layout._asdict()
                }
            }

        return self._metadata_dict
