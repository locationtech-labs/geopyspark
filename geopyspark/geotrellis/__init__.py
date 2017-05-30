"""This subpackage contains the code that reads, writes, and processes data using GeoTrellis."""
from collections import namedtuple
from shapely.geometry import box


class Extent(namedtuple("Extent", 'xmin ymin xmax ymax')):
    """
    The "bounding box" or geographic region of an area on Earth a raster represents.

    Args:
        xmin (float): The minimum x coordinate.
        ymin (float): The minimum y coordinate.
        xmax (float): The maximum x coordinate.
        ymax (float): The maximum y coordinate.

    Attributes:
        xmin (float): The minimum x coordinate.
        ymin (float): The minimum y coordinate.
        xmax (float): The maximum x coordinate.
        ymax (float): The maximum y coordinate.
    """

    __slots__ = []

    @classmethod
    def from_polygon(cls, polygon):
        """Creates a new instance of ``Extent`` from a Shapely Polygon.

        The new ``Extent`` will contain the min and max coordinates of the Polygon;
        regardless of the Polygon's shape.

        Args:
            polygon (shapely.geometry.Polygon): A Shapely Polygon.

        Returns:
            :class:`~geopyspark.geotrellis.Extent`
        """

        return cls(*polygon.bounds)

    @property
    def to_polygon(self):
        """Converts this instance to a Shapely Polygon.

        The resulting Polygon will be in the shape of a box.

        Returns:
            ``shapely.geometry.Polygon``
        """

        return box(*self)


class ProjectedExtent(namedtuple("ProjectedExtent", 'extent epsg proj4')):
    __slots__ = []

    def __new__(cls, extent, epsg=None, proj4=None):
        return super(ProjectedExtent, cls).__new__(cls, extent, epsg, proj4)

    def _asdict(self):
        if isinstance(self.extent, dict):
            return {'extent': self.extent, 'epsg': self.epsg, 'proj4': self.proj4}
        else:
            return {'extent': self.extent._asdict(), 'epsg': self.epsg, 'proj4': self.proj4}


class TemporalProjectedExtent(namedtuple("TemporalProjectedExtent", 'extent instant epsg proj4')):
    __slots__ = []

    def __new__(cls, extent, instant, epsg=None, proj4=None):
        return super(TemporalProjectedExtent, cls).__new__(cls, extent, instant, epsg, proj4)

    def _asdict(self):
        if isinstance(self.extent, dict):
            return {'extent': self.extent, 'instant': self.instant, 'epsg': self.epsg,
                    'proj4': self.proj4}
        else:
            return {'extent': self.extent._asdict(), 'instant': self.instant, 'epsg': self.epsg,
                    'proj4': self.proj4}


TileLayout = namedtuple("TileLayout", 'layoutCols layoutRows tileCols tileRows')
"""
Describes the grid in which the rasters within a RDD should be laid out.

Args:
    layoutCols (int): The number of columns of rasters that runs east to west.
    layoutRows (int): The number of rows of rasters that runs north to south.
    tileCols (int): The number of columns of pixels in each raster that runs east to west.
    tileRows (int): The number of rows of pixels in each raster that runs north to south.

Returns:
    :obj:`~geopyspark.geotrellis.TileLayout`
"""

LayoutDefinition = namedtuple("LayoutDefinition", 'extent tileLayout')
"""
Describes the layout of the rasters within a RDD and how they are projected.

Args:
    extent (:class:`~geopyspark.geotrellis.Extent`): The ``Extent`` of the layout.
    tileLayout (:obj:`~geopyspark.geotrellis.TileLayout`): The ``TileLayout`` of
        how the rasters within the RDD.

Returns:
    :obj:`~geopyspark.geotrellis.LayoutDefinition`
"""

SpatialKey = namedtuple("SpatialKey", 'col row')


SpaceTimeKey = namedtuple("SpaceTimeKey", 'col, row instant')


class Bounds(namedtuple("Bounds", 'minKey maxKey')):
    """
    Represents the grid that covers the area of the rasters in a RDD on a grid.

    Args:
        minKey (:ref:`spatial-key` or :ref:`space-time-key`): The smallest ``SpatialKey`` or
            ``SpaceTimeKey``.
        maxKey (:ref:`spatial-key` or :ref:`space-time-key`): The largest ``SpatialKey`` or
            ``SpaceTimeKey``.

    Returns:
        :cls:`~geopyspark.geotrellis.Bounds`
    """

    __slots__ = []

    def _asdict(self):
        if isinstance(self.minKey, dict):
            min_key_dict = self.minKey
        else:
            min_key_dict = self.minKey._asdict()

        if isinstance(self.maxKey, dict):
            max_key_dict = self.maxKey
        else:
            max_key_dict = self.maxKey._asdict()

        return {'minKey': min_key_dict, 'maxKey': max_key_dict}


class Metadata(object):
    """Information of the values within a ``RasterRDD`` or ``TiledRasterRDD``.
    This data pertains to the layout and other attributes of the data within the classes.

    Args:
        bounds (:obj:`~geopyspark.geotrellis.Bounds`): The ``Bounds`` of the
            values in the class.
        crs (str or int): The ``CRS`` of the data. Can either be the EPSG code, well-known name, or
            a PROJ.4 projection string.
        cell_type (str): The data type of the cells of the rasters.
        extent (:class:`~geopyspark.geotrellis.Extent`): The ``Extent`` that covers
            the all of the rasters.
        layout_definition (:obj:`~geopyspark.geotrellis.LayoutDefinition`): The
            ``LayoutDefinition`` of all rasters.

    Attributes:
        bounds (:obj:`~geopyspark.geotrellis.Bounds`): The ``Bounds`` of the values in the class.
        crs (str or int): The CRS of the data. Can either be the EPSG code, well-known name, or
            a PROJ.4 projection string.
        cell_type (str): The data type of the cells of the rasters.
        extent (:class:`~geopyspark.geotrellis.Extent`): The ``Extent`` that covers
            the all of the rasters.
        tile_layout (:obj:`~geopyspark.geotrellis.TileLayout`): The ``TileLayout``
            that describes how the rasters are orginized.
        layout_definition (:obj:`~geopyspark.geotrellis.LayoutDefinition`): The
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
            metadata_dict (dict): The ``Metadata`` of a ``RasterRDD`` or ``TiledRasterRDD``
                instance that is in ``dict`` form.

        Returns:
            :class:`~geopyspark.geotrellis.Metadata`
        """

        cls._metadata_dict = metadata_dict

        crs = metadata_dict['crs']
        cell_type = metadata_dict['cellType']

        bounds_dict = metadata_dict['bounds']

        if len(bounds_dict['minKey']) == 2:
            min_key = SpatialKey(**bounds_dict['minKey'])
            max_key = SpatialKey(**bounds_dict['maxKey'])
        else:
            min_key = SpaceTimeKey(**bounds_dict['minKey'])
            max_key = SpaceTimeKey(**bounds_dict['maxKey'])

        bounds = Bounds(min_key, max_key)
        extent = Extent(**metadata_dict['extent'])

        layout_definition = LayoutDefinition(
            Extent(**metadata_dict['layoutDefinition']['extent']),
            TileLayout(**metadata_dict['layoutDefinition']['tileLayout']))

        return cls(bounds, crs, cell_type, extent, layout_definition)

    def to_dict(self):
        """Converts this instance to a ``dict``.

        Returns:
            ``dict``
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
