"""Methods for reading, querying, and saving tile layers to and from GeoTrellis Catalogs.
"""

import json
from collections import namedtuple
from urllib.parse import urlparse
from shapely.geometry import Polygon, MultiPolygon, Point
from shapely.wkt import dumps
import shapely.wkb
import pytz

from geopyspark import map_key_input, get_spark_context, scala_companion
from geopyspark.geotrellis.constants import LayerType, IndexingMethod, TimeUnit
from geopyspark.geotrellis.protobufcodecs import multibandtile_decoder
from geopyspark.geotrellis import Metadata, Extent, deprecated, Log
from geopyspark.geotrellis.layer import TiledRasterLayer


__all__ = ["read_layer_metadata", "get_layer_ids", "read_value", "query", "write", "AttributeStore"]


_mapped_cached = {}
_mapped_serializers = {}
_cached = namedtuple('Cached', ('store', 'reader', 'value_reader', 'writer'))

_mapped_bounds = {}


def _construct_catalog(pysc, new_uri, options):
    if new_uri not in _mapped_cached:

        store_factory = pysc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        reader_factory = pysc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
        value_reader_factory = pysc._gateway.jvm.geopyspark.geotrellis.io.ValueReaderFactory
        writer_factory = pysc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory

        parsed_uri = urlparse(new_uri)
        backend = parsed_uri.scheme

        if backend == 'hdfs':
            store = store_factory.buildHadoop(new_uri, pysc._jsc.sc())
            reader = reader_factory.buildHadoop(store, pysc._jsc.sc())
            value_reader = value_reader_factory.buildHadoop(store)
            writer = writer_factory.buildHadoop(store)

        elif backend == 'file':
            store = store_factory.buildFile(new_uri[7:])
            reader = reader_factory.buildFile(store, pysc._jsc.sc())
            value_reader = value_reader_factory.buildFile(store)
            writer = writer_factory.buildFile(store)

        elif backend == 's3':
            store = store_factory.buildS3(parsed_uri.netloc, parsed_uri.path[1:])
            reader = reader_factory.buildS3(store, pysc._jsc.sc())
            value_reader = value_reader_factory.buildS3(store)
            writer = writer_factory.buildS3(store)

        elif backend == 'cassandra':
            parameters = parsed_uri.query.split('&')
            parameter_dict = {}

            for param in parameters:
                split_param = param.split('=', 1)
                parameter_dict[split_param[0]] = split_param[1]

            store = store_factory.buildCassandra(
                parameter_dict['host'],
                parameter_dict['username'],
                parameter_dict['password'],
                parameter_dict['keyspace'],
                parameter_dict['table'],
                options)

            reader = reader_factory.buildCassandra(store, pysc._jsc.sc())
            value_reader = value_reader_factory.buildCassandra(store)
            writer = writer_factory.buildCassandra(store,
                                                   parameter_dict['keyspace'],
                                                   parameter_dict['table'])

        elif backend == 'hbase':

            # The assumed uri looks like: hbase://zoo1, zoo2, ..., zooN: port/table
            (zookeepers, port) = parsed_uri.netloc.split(':')
            table = parsed_uri.path

            if 'master' in options:
                master = options['master']
            else:
                master = ""

            store = store_factory.buildHBase(zookeepers, master, port, table)
            reader = reader_factory.buildHBase(store, pysc._jsc.sc())
            value_reader = value_reader_factory.buildHBase(store)
            writer = writer_factory.buildHBase(store, table)

        elif backend == 'accumulo':

            # The assumed uri looks like: accumulo://username:password/zoo1, zoo2/instance/table
            (user, password) = parsed_uri.netloc.split(':')
            split_parameters = parsed_uri.path.split('/')[1:]

            store = store_factory.buildAccumulo(split_parameters[0],
                                                split_parameters[1],
                                                user,
                                                password,
                                                split_parameters[2])

            reader = reader_factory.buildAccumulo(split_parameters[1],
                                                  store,
                                                  pysc._jsc.sc())

            value_reader = value_reader_factory.buildAccumulo(store)

            writer = writer_factory.buildAccumulo(split_parameters[1],
                                                  store,
                                                  split_parameters[2])

        else:
            raise ValueError("Cannot find Attribute Store for", backend)

        _mapped_cached[new_uri] = _cached(store=store,
                                          reader=reader,
                                          value_reader=value_reader,
                                          writer=writer)


def _in_bounds(layer_type, uri, layer_name, zoom_level, col, row):
    if (layer_name, zoom_level) not in _mapped_bounds:
        layer_metadata = read_layer_metadata(layer_type, uri, layer_name, zoom_level)
        bounds = layer_metadata.bounds
        _mapped_bounds[(layer_name, zoom_level)] = bounds
    else:
        bounds = _mapped_bounds[(layer_name, zoom_level)]

    mins = col < bounds.minKey.col or row < bounds.minKey.row
    maxs = col > bounds.maxKey.col or row > bounds.maxKey.row

    if mins or maxs:
        return False
    else:
        return True


def read_layer_metadata(layer_type,
                        uri,
                        layer_name,
                        layer_zoom,
                        options=None,
                        **kwargs):
    """Reads the metadata from a saved layer without reading in the whole layer.

    Args:
        layer_type (str or :class:`geopyspark.geotrellis.constants.LayerType`): What the spatial type
            of the geotiffs are. This is represented by either constants within ``LayerType`` or by
            a string.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be read from.
        layer_zoom (int): The zoom level of the layer that is to be read.
        options (dict, optional): Additional parameters for reading the layer for specific backends.
            The dictionary is only used for ``Cassandra`` and ``HBase``, no other backend requires
            this to be set.
        **kwargs: The optional parameters can also be set as keywords arguments. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        :class:`~geopyspark.geotrellis.Metadata`
    """

    options = options or kwargs or {}

    _construct_catalog(get_spark_context(), uri, options)
    cached = _mapped_cached[uri]

    if layer_type == LayerType.SPATIAL:
        metadata = cached.store.metadataSpatial(layer_name, layer_zoom)
    else:
        metadata = cached.store.metadataSpaceTime(layer_name, layer_zoom)

    return Metadata.from_dict(json.loads(metadata))


def get_layer_ids(uri,
                  options=None,
                  **kwargs):
    """Returns a list of all of the layer ids in the selected catalog as dicts that contain the
    name and zoom of a given layer.

    Args:
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        options (dict, optional): Additional parameters for reading the layer for specific backends.
            The dictionary is only used for ``Cassandra`` and ``HBase``, no other backend requires this
            to be set.
        **kwargs: The optional parameters can also be set as keywords arguments. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        [layerIds]

        Where ``layerIds`` is a ``dict`` with the following fields:
            - **name** (str): The name of the layer
            - **zoom** (int): The zoom level of the given layer.
    """

    options = options or kwargs or {}

    _construct_catalog(get_spark_context(), uri, options)
    cached = _mapped_cached[uri]

    return list(cached.reader.layerIds())


@deprecated
def read(layer_type,
         uri,
         layer_name,
         layer_zoom,
         options=None,
         num_partitions=None,
         **kwargs):
    """Deprecated in favor of :meth:`~geopyspark.geotrellis.catalog.query`."""

    return query(layer_type, uri, layer_name, layer_zoom, options=options,
                 num_partitions=num_partitions)

def read_value(layer_type,
               uri,
               layer_name,
               layer_zoom,
               col,
               row,
               zdt=None,
               options=None,
               **kwargs):
    """Reads a single ``Tile`` from a GeoTrellis catalog.
    Unlike other functions in this module, this will not return a ``TiledRasterLayer``, but rather a
    GeoPySpark formatted raster. This is the function to use when creating a tile server.

    Note:
        When requesting a tile that does not exist, ``None`` will be returned.

    Args:
        layer_type (str or :class:`geopyspark.geotrellis.constants.LayerType`): What the spatial type
            of the geotiffs are. This is represented by either constants within ``LayerType`` or by
            a string.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be read from.
        layer_zoom (int): The zoom level of the layer that is to be read.
        col (int): The col number of the tile within the layout. Cols run east to west.
        row (int): The row number of the tile within the layout. Row run north to south.
        zdt (``datetime.datetime``): The time stamp of the tile if the data is spatial-temporal.
            This is represented as a ``datetime.datetime.`` instance.  The default value is,
            ``None``. If ``None``, then only the spatial area will be queried.
        options (dict, optional): Additional parameters for reading the tile for specific backends.
            The dictionary is only used for ``Cassandra`` and ``HBase``, no other backend requires
            this to be set.
        **kwargs: The optional parameters can also be set as keywords arguments. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        :class:`~geopyspark.geotrellis.Tile`
    """

    if not _in_bounds(layer_type, uri, layer_name, layer_zoom, col, row):
        return None
    else:
        options = options or kwargs or {}

        if zdt:
            zdt = zdt.astimezone(pytz.utc).isoformat()
        else:
            zdt = ''

        if uri not in _mapped_cached:
            _construct_catalog(get_spark_context(), uri, options)

        cached = _mapped_cached[uri]

        key = map_key_input(LayerType(layer_type).value, True)

        values = cached.value_reader.readTile(key,
                                              layer_name,
                                              layer_zoom,
                                              col,
                                              row,
                                              zdt)

        return multibandtile_decoder(values)


def query(layer_type,
          uri,
          layer_name,
          layer_zoom=None,
          query_geom=None,
          time_intervals=None,
          query_proj=None,
          options=None,
          num_partitions=None,
          **kwargs):
    """Queries a single, zoom layer from a GeoTrellis catalog given spatial and/or time parameters.
    Unlike read, this method will only return part of the layer that intersects the specified
    region.

    Note:
        The whole layer could still be read in if ``intersects`` and/or ``time_intervals`` have not
        been set, or if the querried region contains the entire layer.

    Args:
        layer_type (str or :class:`geopyspark.geotrellis.constants.LayerType`): What the spatial type
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

            If not specified, then the entire layer will be read.
        time_intervals (``[datetime.datetime]``, optional): A list of the time intervals to query.
            This parameter is only used when querying spatial-temporal data. The default value is,
            ``None``. If ``None``, then only the spatial area will be querried.
        query_proj (int or str, optional): The crs of the querried geometry if it is different
            than the layer it is being filtered against. If they are different and this is not set,
            then the returned ``TiledRasterLayer`` could contain incorrect values. If ``None``,
            then the geometry and layer are assumed to be in the same projection.
        options (dict, optional): Additional parameters for querying the tile for specific backends.
            The dictioanry is only used for ``Cassandra`` and ``HBase``, no other backend requires
            this to be set.
        num_partitions (int, optional): Sets RDD partition count when reading from catalog.
        **kwargs: The optional parameters can also be set as keywords arguements. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        :class:`~geopyspark.geotrellis.rdd.TiledRasterLayer`
    """

    options = options or kwargs or {}
    layer_zoom = layer_zoom or 0

    pysc = get_spark_context()

    _construct_catalog(pysc, uri, options)

    cached = _mapped_cached[uri]

    key = map_key_input(LayerType(layer_type).value, True)

    num_partitions = num_partitions or pysc.defaultMinPartitions

    if not query_geom:
        srdd = cached.reader.read(key, layer_name, layer_zoom, num_partitions)
        return TiledRasterLayer(layer_type, srdd)

    else:
        if time_intervals:
            time_intervals = [time.astimezone(pytz.utc).isoformat() for time in time_intervals]
        else:
            time_intervals = []

        query_proj = query_proj or ""

        if isinstance(query_proj, int):
            query_proj = str(query_proj)

        if isinstance(query_geom, (Polygon, MultiPolygon, Point)):
            srdd = cached.reader.query(key,
                                       layer_name,
                                       layer_zoom,
                                       shapely.wkb.dumps(query_geom),
                                       time_intervals,
                                       query_proj,
                                       num_partitions)

        elif isinstance(query_geom, Extent):
            srdd = cached.reader.query(key,
                                       layer_name,
                                       layer_zoom,
                                       shapely.wkb.dumps(query_geom.to_polygon),
                                       time_intervals,
                                       query_proj,
                                       num_partitions)

        elif isinstance(query_geom, bytes):
            srdd = cached.reader.query(key,
                                       layer_name,
                                       layer_zoom,
                                       query_geom,
                                       time_intervals,
                                       query_proj,
                                       num_partitions)
        else:
            raise TypeError("Could not query intersection", query_geom)

        return TiledRasterLayer(layer_type, srdd)


def write(uri,
          layer_name,
          tiled_raster_layer,
          index_strategy=IndexingMethod.ZORDER,
          time_unit=None,
          options=None,
          **kwargs):
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
        options (dict, optional): Additional parameters for writing the layer for specific
            backends. The dictioanry is only used for ``Cassandra`` and ``HBase``, no other backend
            requires this to be set.
        **kwargs: The optional parameters can also be set as keywords arguements. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.
    """

    if not tiled_raster_layer.zoom_level:
        Log.warn(tiled_raster_layer.pysc, "The given layer doesn't not have a zoom_level. Writing to zoom 0.")

    options = options or kwargs or {}
    time_unit = time_unit or ""

    _construct_catalog(tiled_raster_layer.pysc, uri, options)

    cached = _mapped_cached[uri]

    if tiled_raster_layer.layer_type == LayerType.SPATIAL:
        cached.writer.writeSpatial(layer_name,
                                   tiled_raster_layer.srdd,
                                   IndexingMethod(index_strategy).value)
    else:
        cached.writer.writeTemporal(layer_name,
                                    tiled_raster_layer.srdd,
                                    TimeUnit(time_unit).value,
                                    IndexingMethod(index_strategy).value)


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
        self.underlying = scala_companion("geotrellis.spark.io.AttributeStore").apply(uri)

    class Attributes(object):
        """Accessor class for all attributes for a given layer"""
        def __init__(self, store, layer_name, layer_zoom):
            self.store = store
            self.layer_name = layer_name
            self.layer_zoom = layer_zoom
            self.layer_id = scala_companion("geotrellis.spark.LayerId").apply(layer_name, layer_zoom or 0)
            self.utils = scala_companion("geopyspark.geotrellis.GeoTrellisUtils")

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
            value_json = self.utils.readAttributeStoreJson(
                self.store.underlying,
                self.layer_id,
                name)
            if value_json:
                return json.loads(value_json)
            else:
                raise KeyError(self.store.uri, self.layer_name, self.layer_zoom, name)

        def write(self, name, value):
            """Write layer attribute value as a dict

            Args:
                name (str): Attribute name
                value (dict): Attribute value
            """
            value_json = json.dumps(value)
            self.utils.writeAttributeStoreJson(
                self.store.underlying,
                self.layer_id,
                name,
                value_json)

        def delete(self, name):
            """Delete attribute by name

            Args:
                name (str): Attribute name
            """
            self.store.underlying.delete(self.layer_id, name)

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
        layers = self.underlying.layerIds()
        util = scala_companion("geopyspark.geotrellis.GeoTrellisUtils")
        return [self.Attributes(self, l.name(), l.zoom()) for l in util.seqToIterable(layers)]

    def delete(self, name, zoom=None):
        """Delete layer and all its attributes

        Args:
            name (str): Layer name
            zoom (int, optional): Layer zoom
        """
        layer_id = scala_companion("geotrellis.spark.LayerId").apply(name, zoom or 0)
        self.underlying.delete(layer_id)
