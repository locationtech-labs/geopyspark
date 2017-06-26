'''
This module contains the ``RasterLayer`` and the ``TiledRasterLayer`` classes. Both of these
classes are wrappers of their Scala counterparts. These will be used in leau of actual PySpark RDDs
when performing operations.
'''
import json
import shapely.wkb
from geopyspark.geotrellis.protobufcodecs import multibandtile_decoder
from geopyspark.geotrellis.protobufserializer import ProtoBufSerializer
from geopyspark.geopyspark_utils import ensure_pyspark
ensure_pyspark()

from geopyspark import map_key_input, create_python_rdd
from pyspark.storagelevel import StorageLevel
from shapely.geometry import Polygon, MultiPolygon
from geopyspark.geotrellis import Metadata
from geopyspark.geotrellis.constants import (RESAMPLE_METHODS,
                                             OPERATIONS,
                                             Operations,
                                             Neighborhoods,
                                             NEIGHBORHOODS,
                                             ResampleMethods,
                                             FLOAT,
                                             SPATIAL,
                                             ClassificationStrategies,
                                             NODATAINT,
                                             CELL_TYPES
                                            )
from geopyspark.geotrellis.neighborhood import Neighborhood


__all__ = ["RasterLayer", "TiledRasterLayer", "Pyramid"]


def _reclassify(srdd, value_map, data_type, boundary_strategy, replace_nodata_with):
    new_dict = {}

    for key, value in value_map.items():
        if isinstance(key, data_type):
            new_dict[key] = value
        elif isinstance(key, (list, tuple)):
            val = value_map[key]
            for k in key:
                new_dict[k] = val
        else:
            raise TypeError("Expected", data_type, "list, or tuple for the key, but the type was", type(key))

    if data_type is int:
        if not replace_nodata_with:
            return srdd.reclassify(new_dict, boundary_strategy, NODATAINT)
        else:
            return srdd.reclassify(new_dict, boundary_strategy, replace_nodata_with)
    else:
        if not replace_nodata_with:
            return srdd.reclassifyDouble(new_dict, boundary_strategy, float('nan'))
        else:
            return srdd.reclassifyDouble(new_dict, boundary_strategy, replace_nodata_with)


class CachableLayer(object):
    """
    Base class for class that wraps a Scala RDD instance through a py4j reference.

    Attributes:
        pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
        srdd (py4j.java_gateway.JavaObject): The coresponding Scala RDD class.
    """

    def __init__(self):
        self.is_cached = False

    def wrapped_rdds(self):
        """
        Returns the list of RDD-containing objects wrapped by this object.
        The default implementation assumes that subclass contains a single
        RDD container, srdd, which implements the persist() and unpersist()
        methods.
        """
        return [self.srdd]

    def cache(self):
        """
        Persist this RDD with the default storage level (C{MEMORY_ONLY}).
        """
        self.persist()
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_ONLY):
        """
        Set this RDD's storage level to persist its values across operations
        after the first time it is computed. This can only be used to assign
        a new storage level if the RDD does not have a storage level set yet.
        If no storage level is specified defaults to (C{MEMORY_ONLY}).
        """

        javaStorageLevel = self.pysc._getJavaStorageLevel(storageLevel)
        self.is_cached = True
        for srdd in self.wrapped_rdds():
            srdd.persist(javaStorageLevel)
        return self

    def unpersist(self):
        """
        Mark the RDD as non-persistent, and remove all blocks for it from
        memory and disk.
        """

        self.is_cached = False
        for srdd in self.wrapped_rdds():
            srdd.unpersist()
        return self

    def getNumPartitions(self):
        """Returns the number of partitions set for the wrapped RDD.

        Returns:
            Int: The number of partitions.
        """

        return self.srdd.rdd().getNumPartitions()

    def count(self):
        """Returns how many elements are within the wrapped RDD.

        Returns:
            Int: The number of elements in the RDD.
        """

        return self.rdd.rdd().count()


class RasterLayer(CachableLayer):
    """A wrapper of a RDD that contains GeoTrellis rasters.

    Represents a layer that wraps a RDD that contains ``(K, V)``. Where ``K`` is either
    :class:`~geopyspark.geotrellis.ProjectedExtent` or
    :class:`~geopyspark.geotrellis.TemporalProjectedExtent` depending on the ``rdd_type`` of the RDD,
    and ``V`` being a :ref:`raster`.

    The data held within this layer has not been tiled. Meaning the data has yet to be
    modified to fit a certain layout. See :ref:`raster_rdd` for more information.

    Args:
        pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: ``SPATIAL`` and ``SPACETIME``.
        srdd (py4j.java_gateway.JavaObject): The coresponding Scala class. This is what allows
            ``RasterLayer`` to access the various Scala methods.

    Attributes:
        pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: ``SPATIAL`` and ``SPACETIME``.
        srdd (py4j.java_gateway.JavaObject): The coresponding Scala class. This is what allows
            ``RasterLayer`` to access the various Scala methods.
    """

    __slots__ = ['pysc', 'rdd_type', 'srdd']

    def __init__(self, pysc, rdd_type, srdd):
        CachableLayer.__init__(self)
        self.pysc = pysc
        self.rdd_type = rdd_type
        self.srdd = srdd

    @classmethod
    def from_numpy_rdd(cls, pysc, rdd_type, numpy_rdd):
        """Create a ``RasterLayer`` from a numpy RDD.

        Args:
            pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
            rdd_type (str): What the spatial type of the geotiffs are. This is
                represented by the constants: ``SPATIAL`` and ``SPACETIME``.
            numpy_rdd (pyspark.RDD): A PySpark RDD that contains tuples of either
                :class:`~geopyspark.geotrellis.ProjectedExtent`\s or
                :class:`~geopyspark.geotrellis.TemporalProjectedExtent`\s and rasters that
                are represented by a numpy array.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.RasterLayer`
        """

        key = map_key_input(rdd_type, False)
        ser = ProtoBufSerializer.create_tuple_serializer(key_type=key)
        reserialized_rdd = numpy_rdd._reserialize(ser)

        if rdd_type == SPATIAL:
            srdd = \
                    pysc._gateway.jvm.geopyspark.geotrellis.ProjectedRasterRDD.fromProtoEncodedRDD(
                        reserialized_rdd._jrdd)
        else:
            srdd = \
                    pysc._gateway.jvm.geopyspark.geotrellis.TemporalRasterRDD.fromProtoEncodedRDD(
                        reserialized_rdd._jrdd)

        return cls(pysc, rdd_type, srdd)

    def to_numpy_rdd(self):
        """Converts a ``RasterLayer`` to a numpy RDD.

        Note:
            Depending on the size of the data stored within the RDD, this can be an exspensive
            operation and should be used with caution.

        Returns:
            ``pyspark.RDD``
        """

        result = self.srdd.toProtoRDD()
        key = map_key_input(self.rdd_type, False)
        ser = ProtoBufSerializer.create_tuple_serializer(key_type=key)

        return create_python_rdd(self.pysc, result, ser)

    def to_tiled_layer(self, extent=None, layout=None, crs=None, tile_size=256,
                       resample_method=ResampleMethods.NEARESTNEIGHBOR):
        """Converts this ``RasterLayer`` to a ``TiledRasterLayer``.

        This method combines :meth:`~geopyspark.geotrellis.rdd.RasterLayer.collect_metadata` and
        :meth:`~geopyspark.geotrellis.rdd.RasterLayer.tile_to_layout` into one step.

        Args:
            extent (:class:`~geopyspark.geotrellis.Extent`, optional): Specify layout extent,
                must also specify layout.
            layout (:obj:`~geopyspark.geotrellis.TileLayout`, optional): Specify tile layout, must
                also specify ``extent``.
            crs (str or int, optional): Ignore CRS from records and use given one instead.
            tile_size (int, optional): Pixel dimensions of each tile, if not using layout.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by the following constants: ``ResampleMethods.NEARESTNEIGHBOR``, ``BILINEAR``,
                ``CUBICCONVOLUTION``, ``LANCZOS``, ``AVERAGE``, ``MODE``, ``MEDIAN``, ``MAX``, and
                ``MIN``. If none is specified, then ``ResampleMethods.NEARESTNEIGHBOR`` is used.

        Note:
            ``extent`` and ``layout`` must both be defined if they are to be used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """

        return self.tile_to_layout(self.collect_metadata(extent, layout, crs, tile_size),
                                   resample_method)

    def convert_data_type(self, new_type, no_data_value=None):
        """Converts the underlying, raster values to a new ``CellType``.

        Args:
            new_type (str): The string representation of the ``CellType`` to convert to. It is
                represented by a constant such as ``INT16``, ``FLOAT64``, etc.
            no_data_value (int or float, optional): The value that should be marked as NoData.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.RasterLayer`

        Raises:
            ValueError: When an unsupported cell type is entered.
            ValueError: If ``no_data_value`` is set and the ``new_type`` contains raw values.
            ValueError: If ``no_data_value`` is set and ``new_type`` is a boolean.
        """

        if new_type not in CELL_TYPES:
            raise ValueError(new_type, "Is not a know Cell Type")

        if no_data_value:
            if 'bool' in new_type:
                raise ValueError("Cannot add user defined types to Bool")
            elif 'raw' in new_type:
                raise ValueError("Cannot add user defined types to raw values")

            no_data_constant = new_type + "ud" + str(no_data_value)

            return RasterLayer(self.pysc, self.rdd_type,
                             self.srdd.convertDataType(no_data_constant))
        else:
            return RasterLayer(self.pysc, self.rdd_type, self.srdd.convertDataType(new_type))

    def collect_metadata(self, extent=None, layout=None, crs=None, tile_size=256):
        """Iterate over the RDD records and generates layer metadata desribing the contained
        rasters.

        Args:
            extent (:class:`~geopyspark.geotrellis.Extent`, optional): Specify layout extent, must
                also specify ``layout``.
            layout (:obj:`~geopyspark.geotrellis.TileLayout`, optional): Specify tile layout, must
                also specify ``extent``.
            crs (str or int, optional): Ignore CRS from records and use given one instead.
            tile_size (int, optional): Pixel dimensions of each tile, if not using ``layout``.

        Note:
            ``extent`` and ``layout`` must both be defined if they are to be used.

        Returns:
            :class:`~geopyspark.geotrellis.Metadata`

        Raises:
            TypeError: If either ``extent`` and ``layout`` is not defined but the other is.
        """

        if extent and not isinstance(extent, dict):
            extent = extent._asdict()

        if layout and not isinstance(layout, dict):
            layout = layout._asdict()

        if not crs:
            crs = ""

        if isinstance(crs, int):
            crs = str(crs)

        if extent and layout:
            json_metadata = self.srdd.collectMetadata(extent, layout, crs)
        elif not extent and not layout:
            json_metadata = self.srdd.collectMetadata(str(tile_size), crs)
        else:
            raise TypeError("Could not collect metadata with {} and {}".format(extent, layout))

        return Metadata.from_dict(json.loads(json_metadata))

    def reproject(self, target_crs, resample_method=ResampleMethods.NEARESTNEIGHBOR):
        """Reproject every individual raster to ``target_crs``, does not sample past tile boundary

        Args:
            target_crs (str or int): The CRS to reproject to. Can either be the EPSG code,
                well-known name, or a PROJ.4 projection string.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by the following constants: ``NEARESTNEIGHBOR``, ``BILINEAR``,
                ``CUBICCONVOLUTION``, ``LANCZOS``, ``AVERAGE``, ``MODE``, ``MEDIAN``, ``MAX``, and
                ``MIN``. If none is specified, then ``NEARESTNEIGHBOR`` is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.RasterLayer`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        if isinstance(target_crs, int):
            target_crs = str(target_crs)

        return RasterLayer(self.pysc, self.rdd_type,
                         self.srdd.reproject(target_crs, resample_method))

    def cut_tiles(self, layer_metadata, resample_method=ResampleMethods.NEARESTNEIGHBOR):
        """Cut tiles to layout. May result in duplicate keys.

        Args:
            layer_metadata (:class:`~geopyspark.geotrellis.Metadata`): The
                ``Metadata`` of the ``RasterLayer`` instance.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by the following constants: ``NEARESTNEIGHBOR``, ``BILINEAR``,
                ``CUBICCONVOLUTION``, ``LANCZOS``, ``AVERAGE``, ``MODE``, ``MEDIAN``, ``MAX``, and
                ``MIN``. If none is specified, then ``NEARESTNEIGHBOR`` is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        if isinstance(layer_metadata, Metadata):
            layer_metadata = layer_metadata.to_dict()

        srdd = self.srdd.cutTiles(json.dumps(layer_metadata), resample_method)
        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def tile_to_layout(self, layer_metadata, resample_method=ResampleMethods.NEARESTNEIGHBOR):
        """Cut tiles to layout and merge overlapping tiles. This will produce unique keys.

        Args:
            layer_metadata (:class:`~geopyspark.geotrellis.Metadata`): The
                ``Metadata`` of the ``RasterLayer`` instance.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by the following constants: ``NEARESTNEIGHBOR``, ``BILINEAR``,
                ``CUBICCONVOLUTION``, ``LANCZOS``, ``AVERAGE``, ``MODE``, ``MEDIAN``, ``MAX``, and
                ``MIN``. If none is specified, then ``NEARESTNEIGHBOR`` is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        if isinstance(layer_metadata, Metadata):
            layer_metadata = layer_metadata.to_dict()

        srdd = self.srdd.tileToLayout(json.dumps(layer_metadata), resample_method)
        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def reclassify(self, value_map, data_type,
                   boundary_strategy=ClassificationStrategies.LESSTHANOREQUALTO,
                   replace_nodata_with=None):
        """Changes the cell values of a raster based on how the data is broken up.

        Args:
            value_map (dict): A ``dict`` whose keys represent values where a break should occur and
                its values are the new value the cells within the break should become.
            data_type (type): The type of the values within the rasters. Can either be ``int`` or
                ``float``.
            boundary_strategy (str, optional): How the cells should be classified along the breaks.
                This is represented by the following constants: ``GREATERTHAN``,
                ``GREATERTHANOREQUALTO``, ``LESSTHAN``, ``LESSTHANOREQUALTO``, and ``EXACT``. If
                unspecified, then ``LESSTHANOREQUALTO`` will be used.
            replace_nodata_with (data_type, optional): When remapping values, nodata values must be
                treated separately.  If nodata values are intended to be replaced during the
                reclassify, this variable should be set to the intended value.  If unspecified,
                nodata values will be preserved.

        NOTE:
            NoData symbolizes a different value depending on if ``data_type`` is ``int`` or
            ``float``. For ``int``, the constant ``NODATAINT`` can be used which represents the
            NoData value for ``int`` in GeoTrellis. For ``float``, ``float('nan')`` is used to
            represent NoData.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.RasterLayer`
        """

        srdd = _reclassify(self.srdd, value_map, data_type, boundary_strategy, replace_nodata_with)

        return RasterLayer(self.pysc, self.rdd_type, srdd)

    def get_min_max(self):
        """Returns the maximum and minimum values of all of the rasters in the layer.

        Returns:
            (float, float)
        """
        min_max = self.srdd.getMinMax()
        return (min_max._1(), min_max._2())

class TiledRasterLayer(CachableLayer):
    """Wraps a RDD of tiled, GeoTrellis rasters.

    Represents a RDD that contains ``(K, V)``. Where ``K`` is either
    :class:`~geopyspark.geotrellis.SpatialKey` or :class:`~geopyspark.geotrellis.SpaceTimeKey`
    depending on the ``rdd_type`` of the RDD, and ``V`` being a :ref:`raster`.

    The data held within the layer is tiled. This means that the rasters have been modified to fit
    a larger layout. For more information, see :ref:`tiled-raster-rdd`.

    Args:
        pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is represented by the
            constants: ``SPATIAL`` and ``SPACETIME``.
        srdd (py4j.java_gateway.JavaObject): The coresponding Scala class. This is what allows
            ``TiledRasterLayer`` to access the various Scala methods.

    Attributes:
        pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is represented by the
            constants: ``SPATIAL` and ``SPACETIME``.
        srdd (py4j.java_gateway.JavaObject): The coresponding Scala class. This is what allows
            ``RasterLayer`` to access the various Scala methods.
    """

    __slots__ = ['pysc', 'rdd_type', 'srdd']

    def __init__(self, pysc, rdd_type, srdd):
        CachableLayer.__init__(self)
        self.pysc = pysc
        self.rdd_type = rdd_type
        self.srdd = srdd

    @property
    def layer_metadata(self):
        """Layer metadata associated with this layer."""
        return Metadata.from_dict(json.loads(self.srdd.layerMetadata()))

    @property
    def zoom_level(self):
        """The zoom level of the Layer. Can be ``None``."""
        return self.srdd.getZoom()

    @classmethod
    def from_numpy_rdd(cls, pysc, rdd_type, numpy_rdd, metadata):
        """Create a ``TiledRasterLayer`` from a numpy RDD.

        Args:
            pysc (pyspark.SparkContext): The ``SparkContext`` being used this session.
            rdd_type (str): What the spatial type of the geotiffs are. This is represented by the
                constants: ``SPATIAL`` and ``SPACETIME``.
            numpy_rdd (pyspark.RDD): A PySpark RDD that contains tuples of either
                :class:`~geopyspark.geotrellis.SpatialKey` or
                :class:`~geopyspark.geotrellis.SpaceTimeKey` and rasters that are represented by a
                numpy array.
            metadata (:class:`~geopyspark.geotrellis.Metadata`): The ``Metadata`` of
                the ``TiledRasterLayer`` instance.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """
        key = map_key_input(rdd_type, True)
        ser = ProtoBufSerializer.create_tuple_serializer(key_type=key)
        reserialized_rdd = numpy_rdd._reserialize(ser)

        if isinstance(metadata, Metadata):
            metadata = metadata.to_dict()

        if rdd_type == SPATIAL:
            srdd = \
                    pysc._gateway.jvm.geopyspark.geotrellis.SpatialTiledRasterRDD.fromProtoEncodedRDD(
                        reserialized_rdd._jrdd, json.dumps(metadata))
        else:
            srdd = \
                    pysc._gateway.jvm.geopyspark.geotrellis.TemporalTiledRasterRDD.fromProtoEncodedRDD(
                        reserialized_rdd._jrdd, json.dumps(metadata))

        return cls(pysc, rdd_type, srdd)

    def to_numpy_rdd(self):
        """Converts a ``TiledRasterLayer`` to a numpy RDD.

        Note:
            Depending on the size of the data stored within the RDD, this can be an exspensive
            operation and should be used with caution.

        Returns:
            ``pyspark.RDD``
        """
        result = self.srdd.toProtoRDD()
        key = map_key_input(self.rdd_type, True)
        ser = ProtoBufSerializer.create_tuple_serializer(key_type=key)

        return create_python_rdd(self.pysc, result, ser)

    def convert_data_type(self, new_type, no_data_value=None):
        """Converts the underlying, raster values to a new ``CellType``.

        Args:
            new_type (str): The string representation of the ``CellType`` to convert to. It is
                represented by a constant such as ``INT16``, ``FLOAT64``, etc.
            no_data_value (int or float, optional): The value that should be marked as NoData.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`

        Raises:
            ValueError: When an unsupported cell type is entered.
            ValueError: If ``no_data_value`` is set and the ``new_type`` contains raw values.
            ValueError: If ``no_data_value`` is set and ``new_type`` is a boolean.
        """

        if new_type not in CELL_TYPES:
            raise ValueError(new_type, "Is not a know Cell Type")

        if no_data_value:
            if 'bool' in new_type:
                raise ValueError("Cannot add user defined types to Bool")
            elif 'raw' in new_type:
                raise ValueError("Cannot add user defined types to raw values")

            no_data_constant = new_type + "ud" + str(no_data_value)

            return TiledRasterLayer(self.pysc, self.rdd_type,
                                  self.srdd.convertDataType(no_data_constant))
        else:
            return TiledRasterLayer(self.pysc, self.rdd_type, self.srdd.convertDataType(new_type))

    def reproject(self, target_crs, extent=None, layout=None, scheme=FLOAT, tile_size=256,
                  resolution_threshold=0.1, resample_method=ResampleMethods.NEARESTNEIGHBOR):
        """Reproject Layer as tiled raster layer, samples surrounding tiles.

        Args:
            target_crs (str or int): The CRS to reproject to. Can either be the EPSG code,
                well-known name, or a PROJ.4 projection string.
            extent (:class:`~geopyspark.geotrellis.Extent`, optional): Specify the layout
                extent, must also specify ``layout``.
            layout (:obj:`~geopyspark.geotrellis.TileLayout`, optional): Specify the tile layout,
                must also specify ``extent``.
            scheme (str, optional): Which LayoutScheme should be used. Represented by the
                constants: ``FLOAT`` and ``ZOOM``. If not specified, then ``FLOAT`` is used.
            tile_size (int, optional): Pixel dimensions of each tile, if not using layout.
            resolution_threshold (double, optional): The percent difference between a cell size
                and a zoom level along with the resolution difference between the zoom level and
                the next one that is tolerated to snap to the lower-resolution zoom.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by the following constants: ``NEARESTNEIGHBOR``, ``BILINEAR``,
                ``CUBICCONVOLUTION``, ``LANCZOS``, ``AVERAGE``, ``MODE``, ``MEDIAN``, ``MAX``, and
                ``MIN``. If none is specified, then ``NEARESTNEIGHBOR`` is used.

        Note:
            ``extent`` and ``layout`` must both be defined if they are to be used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`

        Raises:
            TypeError: If either ``extent`` or ``layout`` is defined but the other is not.
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        if extent and not isinstance(extent, dict):
            extent = extent._asdict()

        if layout and not isinstance(layout, dict):
            layout = layout._asdict()

        if isinstance(target_crs, int):
            target_crs = str(target_crs)

        if extent and layout:
            srdd = self.srdd.reproject(extent, layout, target_crs, resample_method)
        elif not extent and not layout:
            srdd = self.srdd.reproject(scheme, tile_size, resolution_threshold,
                                       target_crs, resample_method)
        else:
            raise TypeError("Could not collect reproject Layer with {} and {}".format(extent, layout))

        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def repartition(self, num_partitions):
        return TiledRasterLayer(self.pysc, self.rdd_type, self.srdd.repartition(num_partitions))

    def lookup(self, col, row):
        """Return the value(s) in the image of a particular ``SpatialKey`` (given by col and row).

        Args:
            col (int): The ``SpatialKey`` column.
            row (int): The ``SpatialKey`` row.

        Returns:
            A list of numpy arrays (the tiles)

        Raises:
            ValueError: If using lookup on a non ``SPATIAL`` ``TiledRasterLayer``.
            IndexError: If col and row are not within the ``TiledRasterLayer``\'s bounds.
        """
        if self.rdd_type != SPATIAL:
            raise ValueError("Only TiledRasterLayers with a rdd_type of Spatial can use lookup()")
        bounds = self.layer_metadata.bounds
        min_col = bounds.minKey.col
        min_row = bounds.minKey.row
        max_col = bounds.maxKey.col
        max_row = bounds.maxKey.row

        if col < min_col or col > max_col:
            raise IndexError("column out of bounds")
        if row < min_row or row > max_row:
            raise IndexError("row out of bounds")

        array_of_tiles = self.srdd.lookup(col, row)

        return [multibandtile_decoder(tile) for tile in array_of_tiles]

    def tile_to_layout(self, layout, resample_method=ResampleMethods.NEARESTNEIGHBOR):
        """Cut tiles to a given layout and merge overlapping tiles. This will produce unique keys.

        Args:
            layout (:obj:`~geopyspark.geotrellis.TileLayout`): Specify the ``TileLayout`` to cut
                to.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by the following constants: ``NEARESTNEIGHBOR``, ``BILINEAR``,
                ``CUBICCONVOLUTION``, ``LANCZOS``, ``AVERAGE``, ``MODE``, ``MEDIAN``, ``MAX``, and
                ``MIN``. If none is specified, then ``NEARESTNEIGHBOR`` is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        if not isinstance(layout, dict):
            layout = layout._asdict()

        srdd = self.srdd.tileToLayout(layout, resample_method)

        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def pyramid(self, end_zoom, start_zoom=None, resample_method=ResampleMethods.NEARESTNEIGHBOR):
        """Creates a pyramid of GeoTrellis layers where each layer reprsents a given zoom.

        Args:
            end_zoom (int): The zoom level where pyramiding should end. Represents
                the level that is most zoomed out.
            start_zoom (int, Optional): The zoom level where pyramiding should begin. Represents
                the level that is most zoomed in. If None, then will use the zoom level from
                :meth:`~geopyspark.geotrellis.rdd.TiledRasterLayer.zoom_level`.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by the following constants: ``NEARESTNEIGHBOR``, ``BILINEAR``,
                ``CUBICCONVOLUTION``, ``LANCZOS``, ``AVERAGE``, ``MODE``, ``MEDIAN``, ``MAX``, and
                ``MIN``. If none is specified, then ``NEARESTNEIGHBOR`` is used.

        Returns:
            ``[TiledRasterLayers]``.

        Raises:
            ValueError: If the given ``resample_method`` is not known.
            ValueError: If the col and row count is not a power of 2.
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        num_cols = self.layer_metadata.tile_layout.tileCols
        num_rows = self.layer_metadata.tile_layout.tileRows

        if (num_cols & (num_cols - 1)) != 0 or (num_rows & (num_rows - 1)) != 0:
            raise ValueError("Tiles must have a col and row count that is a power of 2")

        if start_zoom:
            start_zoom = start_zoom
        elif self.zoom_level:
            start_zoom = self.zoom_level
        else:
            raise ValueError("No start_zoom given.")

        result = self.srdd.pyramid(start_zoom, end_zoom, resample_method)

        return Pyramid([TiledRasterLayer(self.pysc, self.rdd_type, srdd) for srdd in result])

    def focal(self, operation, neighborhood=None, param_1=None, param_2=None, param_3=None):
        """Performs the given focal operation on the layers contained in the Layer.

        Args:
            operation (str): The focal operation. Represented by constants: ``SUM``, ``MIN``,
                ``MAX``, ``MEAN``, ``MEDIAN``, ``MODE``, ``STANDARDDEVIATION``, ``ASPECT``, and
                ``SLOPE``.
            neighborhood (str or :class:`~geopyspark.geotrellis.neighborhood.Neighborhood`, optional):
                The type of neighborhood to use in the focal operation. This can be represented by
                either an instance of ``Neighborhood``, or by the constants: ``ANNULUS``, ``NEWS``,
                ``SQUARE``, ``WEDGE``, and ``CIRCLE``. Defaults to ``None``.
            param_1 (int or float, optional): If using ``SLOPE``, then this is the zFactor, else it
                is the first argument of ``neighborhood``.
            param_2 (int or float, optional): The second argument of the ``neighborhood``.
            param_3 (int or float, optional): The third argument of the ``neighborhood``.

        Note:
            ``param`` only need to be set if ``neighborhood`` is not an instance of
            ``Neighborhood`` or if ``neighborhood`` is ``None``.

            Any ``param`` that is not set will default to 0.0.

            If ``neighborhood`` is ``None`` then ``operation`` **must** be either ``SLOPE`` or
            ``ASPECT``.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`

        Raises:
            ValueError: If ``operation`` is not a known operation.
            ValueError: If ``neighborhood`` is not a known neighborhood.
            ValueError: If ``neighborhood`` was not set, and ``operation`` is not ``SLOPE`` or
                ``ASPECT``.
        """

        if operation not in OPERATIONS:
            raise ValueError(operation, "Is not a known operation.")

        if isinstance(neighborhood, Neighborhood):
            srdd = self.srdd.focal(operation, neighborhood.name, neighborhood.param_1,
                                   neighborhood.param_2, neighborhood.param_3)

        elif isinstance(neighborhood, str):
            if neighborhood not in NEIGHBORHOODS:
                raise ValueError(neighborhood, "is not a known neighborhood.")

            if param_1 is None:
                param_1 = 0.0
            if param_2 is None:
                param_2 = 0.0
            if param_3 is None:
                param_3 = 0.0

            srdd = self.srdd.focal(operation, neighborhood, float(param_1), float(param_2),
                                   float(param_3))

        elif not neighborhood and operation == Operations.SLOPE or operation == Operations.ASPECT:
            srdd = self.srdd.focal(operation, Neighborhoods.SQUARE, 1.0, 0.0, 0.0)

        else:
            raise ValueError("neighborhood must be set or the operation must be SLOPE or ASPECT")

        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def stitch(self):
        """Stitch all of the rasters within the Layer into one raster.

        Note:
            This can only be used on ``SPATIAL`` ``TiledRasterLayers``.

        Returns:
            :ref:`raster`
        """

        if self.rdd_type != SPATIAL:
            raise ValueError("Only TiledRasterLayers with a rdd_type of Spatial can use stitch()")

        value = self.srdd.stitch()
        ser = ProtoBufSerializer.create_value_serializer("Tile")
        return ser.loads(value)[0]

    def save_stitched(self, path, crop_bounds=None, crop_dimensions=None):
        """Stitch all of the rasters within the Layer into one raster.

        Args:
            path: The path of the geotiff to save.
            crop_bounds: Optional bounds with which to crop the raster before saving.
            crop_dimensions: Optional cols and rows of the image to save

        Note:
            This can only be used on `SPATIAL` TiledRasterLayers.

        Returns:
            None
        """

        if self.rdd_type != SPATIAL:
            raise ValueError("Only TiledRasterLayers with a rdd_type of Spatial can use stitch()")

        if crop_bounds:
            if crop_dimensions:
                self.srdd.save_stitched(path, list(crop_bounds), list(crop_dimensions))
            else:
                self.srdd.save_stitched(path, list(crop_bounds))
        elif crop_dimensions:
            raise Exception("crop_dimensions requires crop_bounds")
        else:
            self.srdd.save_stitched(path)

        return None

    def mask(self, geometries):
        """Masks the ``TiledRasterLayer`` so that only values that intersect the geometries will
        be available.

        Args:
            geometries (shapely.geometry or [shapely.geometry]): Either a list of, or a single
                shapely geometry/ies to use for the mask/s.

                Note:
                    All geometries must be in the same CRS as the TileLayer.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """

        if not isinstance(geometries, list):
            geometries = [geometries]
        wkbs = [shapely.wkb.dumps(g) for g in geometries]
        srdd = self.srdd.mask(wkbs)

        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def reclassify(self, value_map, data_type,
                   boundary_strategy=ClassificationStrategies.LESSTHANOREQUALTO,
                   replace_nodata_with=None):
        """Changes the cell values of a raster based on how the data is broken up.

        Args:
            value_map (dict): A ``dict`` whose keys represent values where a break should occur and
                its values are the new value the cells within the break should become.
            data_type (type): The type of the values within the rasters. Can either be ``int`` or
                ``float``.
            boundary_strategy (str, optional): How the cells should be classified along the breaks.
                This is represented by the following constants: ``GREATERTHAN``,
                ``GREATERTHANOREQUALTO``, ``LESSTHAN``, ``LESSTHANOREQUALTO``, and ``EXACT``. If
                unspecified, then ``LESSTHANOREQUALTO`` will be used.
            replace_nodata_with (data_type, optional): When remapping values, nodata values must be
                treated separately.  If nodata values are intended to be replaced during the
                reclassify, this variable should be set to the intended value.  If unspecified,
                nodata values will be preserved.

        NOTE:
            NoData symbolizes a different value depending on if ``data_type`` is ``int`` or
            ``float``. For ``int``, the constant ``NODATAINT`` can be used which represents the
            NoData value for ``int`` in GeoTrellis. For ``float``, ``float('nan')`` is used to
            represent NoData.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """

        srdd = _reclassify(self.srdd, value_map, data_type, boundary_strategy, replace_nodata_with)

        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def get_min_max(self):
        """Returns the maximum and minimum values of all of the rasters in the Layer.

        Returns:
            ``(float, float)``
        """
        min_max = self.srdd.getMinMax()
        return (min_max._1(), min_max._2())

    def normalize(self, old_min, old_max, new_min, new_max):
        """Finds the min value that is contained within the given geometry.

        Args:
            old_min (float): Old minimum.
            old_max (float): Old maximum.
            new_min (float): New minimum to normalize to.
            new_max (float): New maximum to normalize to.
        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
        """
        srdd = self.srdd.normalize(old_min, old_max, new_min, new_max)

        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    @staticmethod
    def _process_polygonal_summary(geometry, operation):
        if isinstance(geometry, (Polygon, MultiPolygon)):
            geometry = shapely.wkb.dumps(geometry)
        if not isinstance(geometry, bytes):
            raise TypeError("Expected geometry to be bytes but given this instead", type(geometry))

        return operation(geometry)

    def polygonal_min(self, geometry, data_type):
        """Finds the min value that is contained within the given geometry.

        Args:
            geometry (shapely.geometry.Polygon or shapely.geometry.MultiPolygon or bytes): A
                Shapely ``Polygon`` or ``MultiPolygon`` that represents the area where the summary
                should be computed; or a WKB representation of the geometry.
            data_type (type): The type of the values within the rasters. Can either be ``int`` or
                ``float``.

        Returns:
            ``int`` or ``float`` depending on ``data_type``.

        Raises:
            TypeError: If ``data_type`` is not an ``int`` or ``float``.
        """

        if data_type is int:
            return self._process_polygonal_summary(geometry, self.srdd.polygonalMin)
        elif data_type is float:
            return self._process_polygonal_summary(geometry, self.srdd.polygonalMinDouble)
        else:
            raise TypeError("data_type must be either int or float.")

    def polygonal_max(self, geometry, data_type):
        """Finds the max value that is contained within the given geometry.

        Args:
            geometry (shapely.geometry.Polygon or shapely.geometry.MultiPolygon or bytes): A
                Shapely ``Polygon`` or ``MultiPolygon`` that represents the area where the summary
                should be computed; or a WKB representation of the geometry.
            data_type (type): The type of the values within the rasters. Can either be ``int`` or
                ``float``.

        Returns:
            ``int`` or ``float`` depending on ``data_type``.

        Raises:
            TypeError: If ``data_type`` is not an ``int`` or ``float``.
        """

        if data_type is int:
            return self._process_polygonal_summary(geometry, self.srdd.polygonalMax)
        elif data_type is float:
            return self._process_polygonal_summary(geometry, self.srdd.polygonalMaxDouble)
        else:
            raise TypeError("data_type must be either int or float.")

    def polygonal_sum(self, geometry, data_type):
        """Finds the sum of all of the values that are contained within the given geometry.

        Args:
            geometry (shapely.geometry.Polygon or shapely.geometry.MultiPolygon or bytes): A
                Shapely ``Polygon`` or ``MultiPolygon`` that represents the area where the summary
                should be computed; or a WKB representation of the geometry.
            data_type (type): The type of the values within the rasters. Can either be ``int`` or
                ``float``.

        Returns:
            ``int`` or ``float`` depending on ``data_type``.

        Raises:
            TypeError: If ``data_type`` is not an ``int`` or ``float``.
        """

        if data_type is int:
            return self._process_polygonal_summary(geometry, self.srdd.polygonalSum)
        elif data_type is float:
            return self._process_polygonal_summary(geometry, self.srdd.polygonalSumDouble)
        else:
            raise TypeError("data_type must be either int or float.")

    def polygonal_mean(self, geometry):
        """Finds the mean of all of the values that are contained within the given geometry.

        Args:
            geometry (shapely.geometry.Polygon or shapely.geometry.MultiPolygon or bytes): A
                Shapely ``Polygon`` or ``MultiPolygon`` that represents the area where the summary
                should be computed; or a WKB representation of the geometry.

        Returns:
            ``float``
        """

        return self._process_polygonal_summary(geometry, self.srdd.polygonalMean)

    def get_quantile_breaks(self, num_breaks):
        """Returns quantile breaks for this Layer.

        Args:
            num_breaks (int): The number of breaks to return.

        Returns:
            ``[float]``
        """
        return list(self.srdd.quantileBreaks(num_breaks))

    def get_quantile_breaks_exact_int(self, num_breaks):
        """Returns quantile breaks for this Layer.
        This version uses the ``FastMapHistogram``, which counts exact integer values.
        If your layer has too many values, this can cause memory errors.

        Args:
            num_breaks (int): The number of breaks to return.

        Returns:
            ``[int]``
        """
        return list(self.srdd.quantileBreaksExactInt(num_breaks))

    def is_floating_point_layer(self):
        """Determines whether the content of the TiledRasterLayer is of floating point type.

        Args:
            None

        Returns:
            [boolean]
        """
        return self.srdd.isFloatingPointLayer()

    def get_histogram(self):
        """Returns an array of Java histogram objects, one for each band of the raster.

        Args:
            None

        Returns:
            An array of Java objects containing the histograms of each band
        """
        if self.is_floating_point_layer:
            histogram = self.srdd.getDoubleHistograms()
        else:
            histogram = self.srdd.getIntHistograms()
        return histogram

    def _process_operation(self, value, operation):
        if isinstance(value, int) or isinstance(value, float):
            srdd = operation(value)
        elif isinstance(value, TiledRasterLayer):
            if self.rdd_type != value.rdd_type:
                raise ValueError("Both TiledRasterLayers need to have the same rdd_type")

            if self.layer_metadata.tile_layout != value.layer_metadata.tile_layout:
                raise ValueError("Both TiledRasterLayers need to have the same layout")

            srdd = operation(value.srdd)
        elif isinstance(value, list):
            srdd = operation(list(map(lambda x: x.srdd, value)))
        else:
            raise TypeError("Local operation cannot be performed with", value)

        return TiledRasterLayer(self.pysc, self.rdd_type, srdd)

    def __add__(self, value):
        return self._process_operation(value, self.srdd.localAdd)

    def __radd__(self, value):
        return self._process_operation(value, self.srdd.localAdd)

    def __sub__(self, value):
        return self._process_operation(value, self.srdd.localSubtract)

    def __rsub__(self, value):
        return self._process_operation(value, self.srdd.reverseLocalSubtract)

    def __mul__(self, value):
        return self._process_operation(value, self.srdd.localMultiply)

    def __rmul__(self, value):
        return self._process_operation(value, self.srdd.localMultiply)

    def __truediv__(self, value):
        return self._process_operation(value, self.srdd.localDivide)

    def __rtruediv__(self, value):
        return self._process_operation(value, self.srdd.reverseLocalDivide)


def _common_entries(*dcts):
    """Zip two dictionaries together by keys"""
    for i in set(dcts[0]).intersection(*dcts[1:]):
        yield (i,) + tuple(d[i] for d in dcts)

# Keep it in non rendered form so we can do map algebra operations to it
class Pyramid(CachableLayer):
    def __init__(self, levels):
        """List of TiledRasterLayers into a coherent pyramid tile pyramid.

        Args:
            levels (list): A list of TiledRasterLayer.

        """

        if isinstance(levels, dict):
            levels = levels
        elif isinstance(levels, list):
            levels = dict([(l.zoom_level, l) for l in levels])
        else:
            raise TypeError("levels are neither list or dictionary")

        self.levels = levels
        self.max_zoom = max(self.levels.keys())
        self.pysc = levels[self.max_zoom].pysc
        self.rdd_type = levels[self.max_zoom].rdd_type
        self.is_cached = False
        self.histogram = None

    def wrapped_rdds(self):
        return [rdd.srdd for rdd in self.levels.values()]

    def get_histogram(self):
        if not self.histogram:
            self.histogram = self.levels[self.max_zoom].get_histogram()
        return self.histogram

    def to_png_pyramid(self, color_ramp=None, color_map=None, num_breaks=10):
        if not color_map and color_ramp:
            hist = self.get_histogram()
            breaks = hist.quantileBreaks(num_breaks)
            color_map = self.pysc._gateway.jvm.geopyspark.geotrellis.Coloring.makeColorMap(breaks, color_ramp)

        return PngRDD(self.levels, color_map)

    def __add__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__add__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__add__(value) for k, l in self.levels.items()})

    def __radd__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__radd__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__radd__(value) for k, l in self.levels.items()})

    def __sub__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__sub__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__sub__(value) for k, l in self.levels.items()})

    def __rsub__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__rsub__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__rsub__(value) for k, l in self.levels.items()})

    def __mul__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__mul__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__mul__(value) for k, l in self.levels.items()})

    def __rmul__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__rmul__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__rmul__(value) for k, l in self.levels.items()})

    def __truediv__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__truediv__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__truediv__(value) for k, l in self.levels.items()})

    def __rtruediv__(self, value):
        if isinstance(value, Pyramid):
            return Pyramid({k: l.__rtruediv__(r) for k, l, r in _common_entries(self.levels, value.levels)})
        else:
            return Pyramid({k: l.__rtruediv__(value) for k, l in self.levels.items()})
