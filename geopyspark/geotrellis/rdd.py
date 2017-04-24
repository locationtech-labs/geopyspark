'''
This module contains the RasterRDD and the TiledRasterRDD classes. Both of these classes are
wrappers of their scala counterparts. These will be used in leau of actual PySpark RDDs when
performing operations.
'''
import json
import shapely.wkt

from shapely.geometry import Polygon
from shapely.wkt import dumps
from geopyspark.geotrellis.constants import (RESAMPLE_METHODS,
                                             OPERATIONS,
                                             NEIGHBORHOODS,
                                             NEARESTNEIGHBOR,
                                             FLOAT,
                                             TILE,
                                             SPATIAL,
                                             LESSTHANOREQUALTO
                                            )

def _reclassify(srdd, value_map, data_type, boundary_strategy):
    new_dict = {}

    for key, value in value_map.items():
        if not isinstance(key, data_type):
            val = value_map[key]
            for k in key:
                new_dict[k] = val
        else:
            new_dict[key] = value

    if data_type is int:
        return srdd.reclassify(new_dict, boundary_strategy)
    else:
        return srdd.reclassifyDouble(new_dict, boundary_strategy)


class RasterRDD(object):
    """A RDD that contains GeoTrellis rasters.

    Represents a RDD that contains (K, V). Where K is either ProjectedExtent or
    TemporalProjectedExtent depending on the `rdd_type` of the RDD, and V being a raster.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: `SPATIAL` and `SPACETIME`.
        srdd (JavaObject): The coresponding scala class. This is what allows RasterRDD to access
            the various scala methods.

    Attributes:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: `SPATIAL` and `SPACETIME`.
        srdd (JavaObject): The coresponding scala class. This is what allows RasterRDD to access
            the various scala methods.
    """

    __slots__ = ['geopysc', 'rdd_type', 'srdd']

    def __init__(self, geopysc, rdd_type, srdd):
        self.geopysc = geopysc
        self.rdd_type = rdd_type
        self.srdd = srdd

    @classmethod
    def from_numpy_rdd(cls, geopysc, rdd_type, numpy_rdd):
        """Create a RasterRDD from a numpy RDD.

        Args:
            geopysc (GeoPyContext): The GeoPyContext being used this session.
            rdd_type (str): What the spatial type of the geotiffs are. This is
                represented by the constants: `SPATIAL` and `SPACETIME`.
            numpy_rdd (RDD): A PySpark RDD that contains tuples of either ProjectedExtents or
                TemporalProjectedExtents and raster that is represented by a numpy array.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.RasterRDD`
        """

        key = geopysc.map_key_input(rdd_type, False)

        schema = geopysc.create_schema(key)
        ser = geopysc.create_tuple_serializer(schema, key_type=None, value_type=TILE)
        reserialized_rdd = numpy_rdd._reserialize(ser)

        if rdd_type == SPATIAL:
            srdd = \
                    geopysc._jvm.geopyspark.geotrellis.ProjectedRasterRDD.fromAvroEncodedRDD(
                        reserialized_rdd._jrdd, schema)
        else:
            srdd = \
                    geopysc._jvm.geopyspark.geotrellis.TemporalRasterRDD.fromAvroEncodedRDD(
                        reserialized_rdd._jrdd, schema)

        return cls(geopysc, rdd_type, srdd)

    def to_numpy_rdd(self):
        """Converts a RasterRDD to a numpy RDD.

        Note:
            Depending on the size of the data stored within the RDD, this can be an exspensive
            operation and should be used with caution.

        Returns:
            RDD
        """

        result = self.srdd.toAvroRDD()
        ser = self.geopysc.create_tuple_serializer(result._2(), value_type=TILE)
        return self.geopysc.create_python_rdd(result._1(), ser)

    def collect_metadata(self, extent=None, layout=None, crs=None, tile_size=256):
        """Iterate over RDD records and generates layer metadata desribing the contained rasters.

        Args:
            extent (:ref:`extent`, optional): Specify layout extent, must also specify layout.
            layout (:ref:`tile_layout`, optional): Specify tile layout, must also specify extent.
            crs (str, int, optional): Ignore CRS from records and use given one instead.
            tile_size (int, optional): Pixel dimensions of each tile, if not using layout.

        Note:
            `extent` and `layout` must both be defined if they are to be used.

        Returns:
            :ref:`metadata`

        Raises:
            TypeError: If either `extent` and `layout` is not defined but the other is.
        """

        if not crs:
            crs = ""

        if extent and layout:
            json_metadata = self.srdd.collectMetadata(extent, layout, crs)
        elif not extent and not layout:
            json_metadata = self.srdd.collectMetadata(str(tile_size), crs)
        else:
            raise TypeError("Could not collect metadata with {} and {}".format(extent, layout))

        return json.loads(json_metadata)

    def reproject(self, target_crs, resample_method=NEARESTNEIGHBOR):
        """Reproject every individual raster to `target_crs`, does not sample past tile boundary

        Args:
            target_crs (int, str): The CRS to reproject to. Can either be the EPSG code,
                well-known name, or a PROJ.4 projection string.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by a constant. If none is specified, then `NEARESTNEIGHBOR`
                is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.RasterRDD`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        return RasterRDD(self.geopysc, self.rdd_type,
                         self.srdd.reproject(target_crs, resample_method))

    def cut_tiles(self, layer_metadata, resample_method=NEARESTNEIGHBOR):
        """Cut tiles to layout. May result in duplicate keys.

        Args:
            layer_metadata (:ref:`metadata`): The metadata of the ``RasterRDD`` instance.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by a constant. If none is specified, then `NEARESTNEIGHBOR`
                is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        srdd = self.srdd.cutTiles(json.dumps(layer_metadata), resample_method)
        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

    def tile_to_layout(self, layer_metadata, resample_method=NEARESTNEIGHBOR):
        """Cut tiles to layout and merge overlapping tiles. This will produce unique keys.

        Args:
            layer_metadata (:ref:`metadata`): The metadata of the ``RasterRDD`` instance.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by a constant. If none is specified, then `NEARESTNEIGHBOR`
                is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        srdd = self.srdd.tileToLayout(json.dumps(layer_metadata), resample_method)
        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

    def reclassify(self, value_map, data_type, boundary_strategy=LESSTHANOREQUALTO):
        """Changes the cell values of a raster based on how the data is broken up.

        Args:
            value_map (dict): A ``dict`` whose keys represent values where a break should occur and
                its values are the new value the cells within the break should become.
            data_type (type): The type of the values within the rasters. Can either be ``int`` or
                ``float``.
            boundary_strategy (str, optional): How the cells should be classified along the breaks.
                If unspecified, then ``LESSTHANOREQUALTO`` will be used.

        NOTE:
            Simbolizing a NoData value differs depending on if the ``data_type`` is an ``int`` or a
            ``float``. For an ``int``, the constant ``NODATAINT`` can be used which represents the
            NoData value for ``int`` in GeoTrellis. If ``float``, then ``float('nan')`` is used to
            represent NoData.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.RasterRDD`
        """

        srdd = _reclassify(self.srdd, value_map, data_type, boundary_strategy)
        return RasterRDD(self.geopysc, self.rdd_type, srdd)


class TiledRasterRDD(object):
    """Holds a RDD of GeoTrellis rasters.

    Represents a RDD that contains (K, V). Where K is either SpatialKey or SpaceTimeKey depending
    on the `rdd_type` of the RDD, and V being a raster.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: `SPATIAL` and `SPACETIME`.
        srdd (JavaObject): The coresponding scala class. This is what allows RasterRDD to access
            the various scala methods.

    Attributes:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: `SPATIAL` and `SPACETIME`.
        srdd (JavaObject): The coresponding scala class. This is what allows RasterRDD to access
            the various scala methods.
    """

    __slots__ = ['geopysc', 'rdd_type', 'srdd']

    def __init__(self, geopysc, rdd_type, srdd):
        self.geopysc = geopysc
        self.rdd_type = rdd_type
        self.srdd = srdd

    @property
    def layer_metadata(self):
        """Layer metadata associated with this layer."""
        return json.loads(self.srdd.layerMetadata())

    @property
    def zoom_level(self):
        """The zoom level of the RDD. Can be `None`."""
        return self.srdd.getZoom()

    @classmethod
    def from_numpy_rdd(cls, geopysc, rdd_type, numpy_rdd, metadata):
        """Create a TiledRasterRDD from a numpy RDD.

        Args:
            geopysc (GeoPyContext): The GeoPyContext being used this session.
            rdd_type (str): What the spatial type of the geotiffs are. This is
                represented by the constants: `SPATIAL` and `SPACETIME`.
            numpy_rdd (RDD): A PySpark RDD that contains tuples of either ProjectedExtents or
                TemporalProjectedExtents and raster that is represented by a numpy array.
            metadata (:ref:`metadata`): The metadata of the ``TiledRasterRDD`` instance.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`
        """
        key = geopysc.map_key_input(rdd_type, True)

        schema = geopysc.create_schema(key)
        ser = geopysc.create_tuple_serializer(schema, key_type=None, value_type=TILE)
        reserialized_rdd = numpy_rdd._reserialize(ser)

        if rdd_type == SPATIAL:
            srdd = \
                    geopysc._jvm.geopyspark.geotrellis.SpatialTiledRasterRDD.fromAvroEncodedRDD(
                        reserialized_rdd._jrdd, schema, json.dumps(metadata))
        else:
            srdd = \
                    geopysc._jvm.geopyspark.geotrellis.TemporalTiledRasterRDD.fromAvroEncodedRDD(
                        reserialized_rdd._jrdd, schema, json.dumps(metadata))

        return cls(geopysc, rdd_type, srdd)

    @classmethod
    def rasterize(cls, geopysc, rdd_type, geometry, extent, crs, cols, rows,
                  fill_value, instant=None):
        """Creates a TiledRasterRDD from a shapely geomety.

        Args:
            geopysc (GeoPyContext): The GeoPyContext being used this session.
            rdd_type (str): What the spatial type of the geotiffs are. This is
                represented by the constants: `SPATIAL` and `SPACETIME`.
            geometry (str, Polygon): The value to be turned into a raster. Can either be a
                string or a ``Polygon``. If the value is a string, it must be the WKT string,
                geometry format.
            extent (:ref:`extent`): The ``extent`` of the new raster.
            crs (str): The CRS the new raster should be in.
            cols (int): The number of cols the new raster should have.
            rows (int): The number of rows the new raster should have.
            fill_value (int): The value to fill the raster with.

                Note:
                    Only the area the raster intersects with the ``extent`` will have this value.
                    Any other area will be filled with GeoTrellis' NoData value for ``int`` which
                    is represented in GeoPySpark as the constant, ``NODATAINT``.
            instant(int, optional): Optional if the data has no time component (ie is ``SPATIAL``).
                Otherwise, it is requires and represents the time stamp of the data.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`

        Raises:
            TypeError: If ``geometry`` is not a ``str`` or a ``Polygon``; or if there was a
                mistach in inputs. Mainly, setting the ``rdd_type`` as ``SPATIAL`` but also setting
                ``instant``.
        """

        if isinstance(geometry, str):
            pass
        elif isinstance(geometry, Polygon):
            geometry = dumps(geometry)
        else:
            raise TypeError(geometry, "Needs to be either a Polygon or a string")

        if instant and rdd_type != SPATIAL:
            srdd = geopysc._jvm.geopyspark.geotrellis.TemporalTiledRasterRDD.rasterize(
                geopysc.sc, geometry, extent, crs, instant, cols, rows, fill_value)
        elif not instant and rdd_type == SPATIAL:
            srdd = geopysc._jvm.geopyspark.geotrellis.SpatialTiledRasterRDD.rasterize(
                geopysc.sc, geometry, extent, crs, cols, rows, fill_value)
        else:
            raise TypeError("Abiguous inputs. Given ", instant, " but rdd_type is SPATIAL")

        return cls(geopysc, rdd_type, srdd)

    def to_numpy_rdd(self):
        """Converts a TiledRasterRDD to a numpy RDD.

        Note:
            Depending on the size of the data stored within the RDD, this can be an exspensive
            operation and should be used with caution.

        Returns:
            RDD
        """
        result = self.srdd.toAvroRDD()
        ser = self.geopysc.create_tuple_serializer(result._2(), value_type=TILE)
        return self.geopysc.create_python_rdd(result._1(), ser)

    def reproject(self, target_crs, extent=None, layout=None, scheme=FLOAT, tile_size=256,
                  resolution_threshold=0.1, resample_method=NEARESTNEIGHBOR):
        """Reproject RDD as tiled raster layer, samples surrounding tiles.

        Args:
            target_crs (int, str): The CRS to reproject to. Can either be the EPSG code,
                well-known name, or a PROJ.4 projection string.
                extent (:ref:`extent`, optional): Specify layout extent, must also specify layout.
            layout (:ref:`tile_layout`, optional): Specify tile layout, must also specify extent.
            scheme (str, optional): Which LayoutScheme should be used. Represented by the
                constants: `FLOAT` and `ZOOM`. If not specified, then `FLOAT` is used.
            tile_size (int, optional): Pixel dimensions of each tile, if not using layout.
            resolution_threshold (double, optional): The percent difference between a cell size
                and a zoom level along with the resolution difference between the zoom level and
                the next one that is tolerated to snap to the lower-resolution zoom.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by a constant. If none is specified, then NEARESTNEIGHBOR
                is used.

        Note:
            `extent` and `layout` must both be defined if they are to be used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`

        Raises:
            TypeError if either `extent` or `layout` is defined bu the other is not.
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        if extent and layout:
            srdd = self.srdd.reproject(extent, layout, target_crs, resample_method)
        elif not extent and not layout:
            srdd = self.srdd.reproject(scheme, tile_size, resolution_threshold,
                                       target_crs, resample_method)
        else:
            raise TypeError("Could not collect reproject RDD with {} and {}".format(extent, layout))

        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

    def tile_to_layout(self, layout, resample_method=NEARESTNEIGHBOR):
        """Cut tiles to layout and merge overlapping tiles. This will produce unique keys.

        Args:
            layout (:ref:`tile_layout`): Specify the TileLayout to cut to.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by a constant. If none is specified, then NEARESTNEIGHBOR
                is used.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        srdd = self.srdd.tileToLayout(layout, resample_method)

        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

    def pyramid(self, start_zoom, end_zoom, resample_method=NEARESTNEIGHBOR):
        """Creates a pyramid of GeoTrellis layers where each layer reprsents a given zoom.

        Args:
            start_zoom (int): The zoom level where pyramiding should begin. Represents
                the level that is most zoomed in.
            end_zoom (int): The zoom level where pyramiding should end. Represents
                the level that is most zoomed out.
            resample_method (str, optional): The resample method to use for the reprojection.
                This is represented by a constant. If none is specified, then NEARESTNEIGHBOR
                is used.

        Returns:
            Pyramided TiledRasterRDDs (list): A list of TiledRasterRDDs.
        """

        if resample_method not in RESAMPLE_METHODS:
            raise ValueError(resample_method, " Is not a known resample method.")

        size = self.layer_metadata['layoutDefinition']['tileLayout']['tileRows']

        if (size & (size - 1)) != 0:
            raise ValueError("Tiles must have a col and row count that is a power of 2")

        result = self.srdd.pyramid(start_zoom, end_zoom, resample_method)

        return [TiledRasterRDD(self.geopysc, self.rdd_type, srdd) for srdd in result]

    def focal(self, operation, neighborhood, param_1=None, param_2=None, param_3=None):
        """Performs the given focal operation on the layers contained in the RDD.

        Args:
            operation (str): The focal operation such as SUM, ASPECT, SLOPE, etc.
            neighborhood (str): The type of neighborhood to use such as ANNULUS, SQUARE, etc.
            param_1 (int, optional): If using SLOPE, then this is the zFactor, else it is the first
                argument of the `neighborhood`.
            param_2 (int, optional): The second argument of the `neighborhood`.
            param_3 (int, optional): The third argument of the `neighborhood`.

        Note:
            Any `param` that is not set will default to 0.0.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`
        """

        if operation not in OPERATIONS:
            raise ValueError(operation, " Is not a known operation.")

        if param_1 is None:
            param_1 = 0.0
        if param_2 is None:
            param_2 = 0.0
        if param_3 is None:
            param_3 = 0.0

        srdd = self.srdd.focal(operation, neighborhood, param_1, param_2, param_3)

        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

    def stitch(self):
        """Stitch all of the rasters within the RDD into one raster.

        Note:
            This can only be used on `SPATIAL` TiledRasterRDDs.

        Returns:
            :ref:`raster`
        """

        if self.rdd_type != SPATIAL:
            raise ValueError("Only TiledRasterRDDs with a rdd_type of Spatial can use stitch()")

        tup = self.srdd.stitch()
        ser = self.geopysc.create_value_serializer(tup._2(), TILE)
        return ser.loads(tup._1())[0]

    def cost_distance(self, geometries, max_distance):
        """Performs cost distance of a TileLayer.

        Args:
            geometries (list): A list of shapely geometries to be used as a starting point.

                Note:
                    All geometries must be in the same CRS as the TileLayer.
            max_distance (int): The maximum ocst that a path may reach before operation.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`
        """

        wkts = [shapely.wkt.dumps(g) for g in geometries]
        srdd = self.srdd.costDistance(self.geopysc.sc, wkts, max_distance)

        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

    def reclassify(self, value_map, data_type, boundary_strategy=LESSTHANOREQUALTO):
        """Changes the cell values of a raster based on how the data is broken up.

        Args:
            value_map (dict): A ``dict`` whose keys represent values where a break should occur and
                its values are the new value the cells within the break should become.
            data_type (type): The type of the values within the rasters. Can either be ``int`` or
                ``float``.
            boundary_strategy (str, optional): How the cells should be classified along the breaks.
                If unspecified, then ``LESSTHANOREQUALTO`` will be used.

        NOTE:
            Simbolizing a NoData value differs depending on if the ``data_type`` is an ``int`` or a
            ``float``. For an ``int``, the constant ``NODATAINT`` can be used which represents the
            NoData value for ``int`` in GeoTrellis. If ``float``, then ``float('nan')`` is used to
            represent NoData.

        Returns:
            :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`
        """

        srdd = _reclassify(self.srdd, value_map, data_type, boundary_strategy)
        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

    def _process_operation(self, value, operation):
        if isinstance(value, int) or isinstance(value, float):
            srdd = operation(value)
        elif isinstance(value, TiledRasterRDD):
            if self.rdd_type != value.rdd_type:
                raise ValueError("Both TiledRasterRDDs need to have the same rdd_type")

            if self.layer_metadata['layoutDefinition']['tileLayout'] != \
               value.layer_metadata['layoutDefinition']['tileLayout']:
                raise ValueError("Both TiledRasterRDDs need to have the same layout")

            srdd = operation(value.srdd)
        else:
            raise TypeError("Local operation cannot be performed with", value)

        return TiledRasterRDD(self.geopysc, self.rdd_type, srdd)

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
