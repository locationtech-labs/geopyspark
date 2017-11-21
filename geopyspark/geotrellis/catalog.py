"""Methods for reading, querying, and saving tile layers to and from GeoTrellis Catalogs.
"""

import json
from shapely.geometry import Polygon, MultiPolygon, Point
import shapely.wkb
import pytz
from py4j.protocol import Py4JJavaError

from geopyspark import get_spark_context, scala_companion
from geopyspark.geotrellis.constants import LayerType, IndexingMethod, TimeUnit
from geopyspark.geotrellis.protobufcodecs import multibandtile_decoder
from geopyspark.geotrellis import Metadata, Extent, deprecated, Log
from geopyspark.geotrellis.layer import TiledRasterLayer


__all__ = ["read_layer_metadata", "read_value", "query", "write", "AttributeStore"]

"""Instances of previously used AttributeStore keyed by their URI """
_cached_stores = {}


def read_layer_metadata(uri,
                        layer_name,
                        layer_zoom):
    """Reads the metadata from a saved layer without reading in the whole layer.

    Args:
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be read from.
        layer_zoom (int): The zoom level of the layer that is to be read.

    Returns:
        :class:`~geopyspark.geotrellis.Metadata`
    """

    store = AttributeStore.cached(uri)
    return store.layer(layer_name, layer_zoom).layer_metadata()


def read_value(uri,
               layer_name,
               layer_zoom,
               col,
               row,
               zdt=None,
               store=None):
    """Reads a single ``Tile`` from a GeoTrellis catalog.
    Unlike other functions in this module, this will not return a ``TiledRasterLayer``, but rather a
    GeoPySpark formatted raster.

    Note:
        When requesting a tile that does not exist, ``None`` will be returned.

    Args:
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be read from.
        layer_zoom (int): The zoom level of the layer that is to be read.
        col (int): The col number of the tile within the layout. Cols run east to west.
        row (int): The row number of the tile within the layout. Row run north to south.
        zdt (``datetime.datetime``): The time stamp of the tile if the data is spatial-temporal.
            This is represented as a ``datetime.datetime.`` instance.  The default value is,
            ``None``. If ``None``, then only the spatial area will be queried.
        store (str or :class:`~geopyspark.geotrellis.catalog.AttributeStore`, optional):
            ``AttributeStore`` instance or URI for layer metadata lookup.

    Returns:
        :class:`~geopyspark.geotrellis.Tile`
    """

    if store:
        store = AttributeStore.build(store)
    else:
        store = AttributeStore.cached(uri)

    reader = ValueReader(uri, layer_name, layer_zoom, store)
    return reader.read(col, row, zdt)


class ValueReader(object):
    """GeoTrellis catalog indivual value reader.
    Suitable for use in TMS service because it does not have Spark overhead.
    """

    def __init__(self, uri, layer_name, zoom=None, store=None):
        if store:
            self.store = AttributeStore.build(store)
        else:
            self.store = AttributeStore.cached(uri)

        self.layer_name = layer_name
        self.zoom = zoom
        pysc = get_spark_context()
        scala_store = self.store.wrapper.attributeStore()
        ValueReaderWrapper = pysc._gateway.jvm.geopyspark.geotrellis.io.ValueReaderWrapper
        self.wrapper = ValueReaderWrapper(scala_store, uri)

    def read(self, col, row, zdt=None, zoom=None):
        """Reads a single ``Tile`` from a GeoTrellis catalog.
           When requesting a tile that does not exist, ``None`` will be returned.

        Args:

            col (int): The col number of the tile within the layout. Cols run east to west.
            row (int): The row number of the tile within the layout. Row run north to south.
            zdt (``datetime.datetime``): The time stamp of the tile if the data is spatial-temporal.
                This is represented as a ``datetime.datetime.`` instance.  The default value is,
                ``None``. If ``None``, then only the spatial area will be queried.
            zoom (int, optional): The zoom level of the layer that is to be read.
                Defaults to ``self.zoom``

        Returns:
            :class:`~geopyspark.geotrellis.Tile`
        """

        zoom = zoom or self.zoom or 0
        zoom = zoom and int(zoom)

        if zdt:
            if zdt.tzinfo:
                zdt = zdt.astimezone(pytz.utc).isoformat()
            else:
                zdt = zdt.replace(tzinfo=pytz.utc).isoformat()

        value = self.wrapper.readTile(self.layer_name, zoom, col, row, zdt)
        return value and multibandtile_decoder(value)


def query(uri,
          layer_name,
          layer_zoom=None,
          query_geom=None,
          time_intervals=None,
          query_proj=None,
          num_partitions=None,
          store=None):
    """Queries a single, zoom layer from a GeoTrellis catalog given spatial and/or time parameters.

    Note:
        The whole layer could still be read in if ``intersects`` and/or ``time_intervals`` have not
        been set, or if the querried region contains the entire layer.

    Args:
        layer_type (str or :class:`~geopyspark.geotrellis.constants.LayerType`): What the layer type
            of the geotiffs are. This is represented by either constants within ``LayerType`` or by
            a string.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be querried.
        layer_zoom (int, optional): The zoom level of the layer that is to be querried.
            If ``None``, then the ``layer_zoom`` will be set to 0.
        query_geom (bytes or shapely.geometry or :class:`~geopyspark.geotrellis.Extent`, Optional):
            The desired spatial area to be returned. Can either be a string, a shapely geometry, or
            instance of ``Extent``, or a WKB verson of the geometry.

            Note:
                Not all shapely geometires are supported. The following is are the types that are
                supported:
                * Point
                * Polygon
                * MultiPolygon

            Note:
                Only layers that were made from spatial, singleband GeoTiffs can query a ``Point``.
                All other types are restricted to ``Polygon`` and ``MulitPolygon``.

            Note:
                If the queried region does not intersect the layer, then an empty layer will be
                returned.

            If not specified, then the entire layer will be read.
        time_intervals (``[datetime.datetime]``, optional): A list of the time intervals to query.
            This parameter is only used when querying spatial-temporal data. The default value is,
            ``None``. If ``None``, then only the spatial area will be querried.
        query_proj (int or str, optional): The crs of the querried geometry if it is different
            than the layer it is being filtered against. If they are different and this is not set,
            then the returned ``TiledRasterLayer`` could contain incorrect values. If ``None``,
            then the geometry and layer are assumed to be in the same projection.
        num_partitions (int, optional): Sets RDD partition count when reading from catalog.
        store (str or :class:`~geopyspark.geotrellis.catalog.AttributeStore`, optional):
            ``AttributeStore`` instance or URI for layer metadata lookup.

    Returns:
        :class:`~geopyspark.geotrellis.layer.TiledRasterLayer`
    """
    if store:
        store = AttributeStore.build(store)
    else:
        store = AttributeStore.cached(uri)

    pysc = get_spark_context()
    layer_zoom = layer_zoom or 0

    if query_geom is None:
        pass  # pass as Null to Java
    elif isinstance(query_geom, Extent):
        query_geom = query_geom.to_polygon
        query_geom = shapely.wkb.dumps(query_geom)
    elif isinstance(query_geom, (Polygon, MultiPolygon, Point)):
        query_geom = shapely.wkb.dumps(query_geom)
    elif isinstance(query_geom, bytes):
        pass  # assume bytes are WKB
    else:
        raise TypeError("Could not query intersection", query_geom)

    if isinstance(query_proj, int):
        query_proj = str(query_proj)

    if time_intervals:
        for x in range(0, len(time_intervals)):
            time = time_intervals[x]
            if time.tzinfo:
                time_intervals[x] = time.astimezone(pytz.utc).isoformat()
            else:
                time_intervals[x] = time.replace(tzinfo=pytz.utc).isoformat()
    else:
        time_intervals = []

    reader = pysc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderWrapper(pysc._jsc.sc())
    scala_store = store.wrapper.attributeStore()
    srdd = reader.query(scala_store, uri,
                        layer_name, layer_zoom,
                        query_geom, time_intervals, query_proj,
                        num_partitions)

    layer_type = LayerType._from_key_name(srdd.keyClassName())

    return TiledRasterLayer(layer_type, srdd)


def write(uri,
          layer_name,
          tiled_raster_layer,
          index_strategy=IndexingMethod.ZORDER,
          time_unit=None,
          time_resolution=None,
          store=None):
    """Writes a tile layer to a specified destination.

    Args:
        uri (str): The Uniform Resource Identifier used to point towards the desired location for
            the tile layer to written to. The shape of this string varies depending on backend.
        layer_name (str): The name of the new, tile layer.
        layer_zoom (int): The zoom level the layer should be saved at.
        tiled_raster_layer (:class:`~geopyspark.geotrellis.layer.TiledRasterLayer`): The
            ``TiledRasterLayer`` to be saved.
        index_strategy (str or :class:`~geopyspark.geotrellis.constants.IndexingMethod`): The
            method used to orginize the saved data. Depending on the type of data within the layer,
            only certain methods are available. Can either be a string or a ``IndexingMethod``
            attribute.  The default method used is, ``IndexingMethod.ZORDER``.
        time_unit (str or :class:`~geopyspark.geotrellis.constants.TimeUnit`, optional): Which time
            unit should be used when saving spatial-temporal data. This controls the resolution of
            each index. Meaning, what time intervals are used to seperate each record. While this is
            set to ``None`` as default, it must be set if saving spatial-temporal data.
            Depending on the indexing method chosen, different time units are used.
        time_resolution (str or int, optional): Determines how data for each ``time_unit`` should be
            grouped together. By default, no grouping will occur.

            As an example, having a ``time_unit`` of ``WEEKS`` and a ``time_resolution`` of 5 will
            cause the data to be grouped and stored together in units of 5 weeks. If however
            ``time_resolution`` is not specified, then the data will be grouped and stored in units
            of single weeks.

            This value can either be an ``int`` or a string representation of an ``int``.
        store (str or :class:`~geopyspark.geotrellis.catalog.AttributeStore`, optional):
            ``AttributeStore`` instance or URI for layer metadata lookup.
    """

    if tiled_raster_layer.zoom_level is None:
        Log.warn(tiled_raster_layer.pysc, "The given layer doesn't not have a zoom_level. Writing to zoom 0.")

    if store:
        store = AttributeStore.build(store)
    else:
        store = AttributeStore.cached(uri)

    pysc = tiled_raster_layer.pysc
    time_unit = time_unit or ""

    writer = pysc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterWrapper(
        store.wrapper.attributeStore(), uri)

    if tiled_raster_layer.layer_type == LayerType.SPATIAL:
        writer.writeSpatial(layer_name,
                            tiled_raster_layer.srdd,
                            IndexingMethod(index_strategy).value)

    elif tiled_raster_layer.layer_type == LayerType.SPACETIME:
        if time_resolution:
            time_resolution = str(time_resolution)

        writer.writeTemporal(layer_name,
                             tiled_raster_layer.srdd,
                             TimeUnit(time_unit).value,
                             time_resolution,
                             IndexingMethod(index_strategy).value)
    else:
        raise ValueError("Cannot write {} layer".format(tiled_raster_layer.layer_type))


class AttributeStore(object):
    """AttributeStore provides a way to read and write GeoTrellis layer attributes.

    Internally all attribute values are stored as JSON, here they are exposed as dictionaries.
    Classes often stored have a ``.from_dict`` and ``.to_dict`` methods to bridge the gap::

        import geopyspark as gps
        store = gps.AttributeStore("s3://azavea-datahub/catalog")
        hist = store.layer("us-nlcd2011-30m-epsg3857", zoom=7).read("histogram")
        hist = gps.Histogram.from_dict(hist)
    """
    def __init__(self, uri):
        self.uri = uri
        pysc = get_spark_context()
        try:
            self.wrapper = pysc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreWrapper(uri)
        except Py4JJavaError as err:
            raise ValueError(err.java_exception.getMessage())

    @classmethod
    def build(cls, store):
        """Builds AttributeStore from URI or passes an instance through.

        Args:
            uri (str or AttributeStore): URI for AttributeStore object or instance.

        Returns:
            :class:`~geopyspark.geotrellis.catalog.AttributeStore`
        """

        if isinstance(store, AttributeStore):
            return store
        elif isinstance(store, str):
            return cls(store)
        else:
            raise ValueError("Cannot make {} into AttributStore".format(store))

    @classmethod
    def cached(cls, uri):
        """Returns cached version of AttributeStore for URI or creates one"""
        if uri in _cached_stores:
            return _cached_stores[uri]
        else:
            store = cls(uri)
            _cached_stores[uri] = store
            return store

    class Attributes(object):
        """Accessor class for all attributes for a given layer"""
        def __init__(self, store, layer_name, layer_zoom):
            self.store = store
            self.layer_name = layer_name
            self.layer_zoom = layer_zoom

        def __repr__(self):
            return "Attributes({}, {}, {})".format(self.store.uri, self.layer_name, self.layer_zoom)

        def __getitem__(self, name):
            return self.read(name)

        def __setitem__(self, name, value):
            return self.write(name, value)

        def __delitem__(self, name):
            return self.delete(name)

        def read(self, name):
            """Read layer attribute by name as a dict

            Args:
                name (str) Attribute name

            Returns:
                ``dict``: Attribute value
            """
            zoom = self.layer_zoom or 0
            value_json = self.store.wrapper.read(self.layer_name, zoom, name)
            if value_json:
                return json.loads(value_json)
            else:
                raise KeyError(self.store.uri, self.layer_name, self.layer_zoom, name)

        def layer_metadata(self):
            zoom = self.layer_zoom or 0
            value_json = self.store.wrapper.readMetadata(self.layer_name, zoom)
            if value_json:
                metadata_dict = json.loads(value_json)
                return Metadata.from_dict(metadata_dict)
            else:
                raise KeyError(self.store.uri, self.layer_name, self.layer_zoom, "layer metadata")

        def write(self, name, value):
            """Write layer attribute value as a dict

            Args:
                name (str): Attribute name
                value (dict): Attribute value
            """
            zoom = self.layer_zoom or 0
            value_json = json.dumps(value)
            self.store.wrapper.write(self.layer_name, zoom, name, value_json)

        def delete(self, name):
            """Delete attribute by name

            Args:
                name (str): Attribute name
            """
            zoom = self.layer_zoom or 0
            self.store.wrapper.delete(self.layer_name, zoom, name)

    def layer(self, name, zoom=None):
        """Layer Attributes object for given layer
        Args:
            name (str): Layer name
            zoom (int, optional): Layer zoom

        Returns:
            :class:`~geopyspark.geotrellis.catalog.Attributes`
        """
        return self.Attributes(self, name, zoom)

    def layers(self):
        """List all layers Attributes objects

        Returns:
            ``[:class:`~geopyspark.geotrellis.catalog.AttributeStore.Attributes`]``
        """
        layers = self.wrapper.attributeStore().layerIds()
        util = scala_companion("geopyspark.geotrellis.GeoTrellisUtils")
        return [self.Attributes(self, l.name(), l.zoom()) for l in util.seqToIterable(layers)]

    def delete(self, name, zoom=None):
        """Delete layer and all its attributes

        Args:
            name (str): Layer name
            zoom (int, optional): Layer zoom
        """
        zoom = zoom or 0
        self.wrapper.delete(name, zoom)

    def contains(self, name, zoom=None):
        """Checks if this store contains a layer metadata.

        Args:
            name (str): Layer name
            zoom (int, optional): Layer zoom

        Returns:
           ``bool``
        """
        zoom = zoom or 0
        return self.wrapper.contains(name, zoom)
