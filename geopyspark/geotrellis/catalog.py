"""Methods for reading, querying, and saving tile layers to and from GeoTrellis Catalogs.

Because GeoPySpark represents all raster data as 3D numpy arrays, data that is read/written out
will be in a multiband format; regardless of how the data was originally formatted.

This module can read, write, and query from a variety of backends.
These are the ones that are currently supported:

 - **Local Filesystem**
 - **HDFS**
 - **S3**
 - **Cassandra**
 - **HBase**
 - **Accumulo**

Example uris for each backend:
 - Local Filesystem: file://my_folder/my_catalog/
 - HDFS: hdfs://my_folder/my_catalog/
 - S3: s3://my_bucket/my_catalog/
 - Cassandra: cassandra:name?username=user&password=pass&host=host1&keyspace=key&table=table
 - HBase: hbase://zoo1, zoo2: port/table
 - Accumulo: accumulo://username:password/zoo1, zoo2/instance/table

Note:
    Neither ``HBase`` or ``Accumulo`` support URI strings to query data. Therefore, URIs for both
    backends have been constructed for use in GeoPySpark.

The URI for ``HBase`` follows this pattern:
 - hbase://zoo1, zoo2, ..., zooN: port/table

The URI for ``Accumulo`` follows this pattern:
 - accumulo://username:password/zoo1, zoo2/instance/table

Some backends require various options to be set in the ``options`` parameter
of each function. These are backends and the additonal options to set for
each.

Fields that can be set for ``Cassandra``:
 - **replicationStrategy** (str, optional): If not specified, then
   'SimpleStrategy' will be used.
 - **replicationFactor** (int, optional): If not specified, then 1 will be used.
 - **localDc** (str, optional): If not specified, then 'datacenter1' will be used.
 - **usedHostsPerRemoteDc** (int, optional): If not specified, then 0 will be used.
 - **allowRemoteDCsForLocalConsistencyLevel** (int, optional): If you'd like this feature,
     then the value would be 1, Otherwise, the value should be 0. If not specified,
     then 0 will be used.

Fields that can be set for ``HBase``:
 - **master** (str, optional): If not specified, then 'null' will be used.
"""

import json
from collections import namedtuple
from urllib.parse import urlparse

from geopyspark.geotrellis import Metadata, Extent
from geopyspark.geotrellis.rdd import TiledRasterRDD
from geopyspark.geotrellis.constants import TILE, ZORDER, SPATIAL

from shapely.geometry import Polygon, MultiPolygon, Point
from shapely.wkt import dumps


_mapped_cached = {}
_mapped_serializers = {}
_cached = namedtuple('Cached', ('store', 'reader', 'value_reader', 'writer'))

_mapped_bounds = {}
_bounds = namedtuple('Bounds', ('col_min', 'row_min', 'col_max', 'row_max'))


def _construct_catalog(geopysc, new_uri, options):
    if new_uri not in _mapped_cached:

        store_factory = geopysc._jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        reader_factory = geopysc._jvm.geopyspark.geotrellis.io.LayerReaderFactory
        value_reader_factory = geopysc._jvm.geopyspark.geotrellis.io.ValueReaderFactory
        writer_factory = geopysc._jvm.geopyspark.geotrellis.io.LayerWriterFactory

        parsed_uri = urlparse(new_uri)
        backend = parsed_uri.scheme

        if backend == 'hdfs':
            store = store_factory.buildHadoop(new_uri, geopysc.sc)
            reader = reader_factory.buildHadoop(store, geopysc.sc)
            value_reader = value_reader_factory.buildHadoop(store)
            writer = writer_factory.buildHadoop(store)

        elif backend == 'file':
            store = store_factory.buildFile(new_uri[7:])
            reader = reader_factory.buildFile(store, geopysc.sc)
            value_reader = value_reader_factory.buildFile(store)
            writer = writer_factory.buildFile(store)

        elif backend == 's3':
            store = store_factory.buildS3(parsed_uri.netloc, parsed_uri.path[1:])
            reader = reader_factory.buildS3(store, geopysc.sc)
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

            reader = reader_factory.buildCassandra(store, geopysc.sc)
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
            reader = reader_factory.buildHBase(store, geopysc.sc)
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
                                                  geopysc.sc)

            value_reader = value_reader_factory.buildAccumulo(store)

            writer = writer_factory.buildAccumulo(split_parameters[1],
                                                  store,
                                                  split_parameters[2])

        else:
            raise ValueError("Cannot find Attribute Store for, {}".format(backend))

        _mapped_cached[new_uri] = _cached(store=store,
                                          reader=reader,
                                          value_reader=value_reader,
                                          writer=writer)

def _in_bounds(geopysc, rdd_type, uri, layer_name, zoom_level, col, row):
    if (layer_name, zoom_level) not in _mapped_bounds:
        layer_metadata = read_layer_metadata(geopysc, rdd_type, uri, layer_name, zoom_level)
        bounds_dict = layer_metadata.bounds
        min_key = bounds_dict.minKey
        max_key = bounds_dict.maxKey
        bounds = _bounds(min_key['col'], min_key['row'], max_key['col'], max_key['row'])
        _mapped_bounds[(layer_name, zoom_level)] = bounds
    else:
        bounds = _mapped_bounds[(layer_name, zoom_level)]

    mins = col < bounds.col_min or row < bounds.row_min
    maxs = col > bounds.col_max or row > bounds.row_max

    if mins or maxs:
        return False
    else:
        return True


def read_layer_metadata(geopysc,
                        rdd_type,
                        uri,
                        layer_name,
                        layer_zoom,
                        options=None,
                        **kwargs):
    """Reads the metadata from a saved layer without reading in the whole layer.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: ``SPATIAL`` and ``SPACETIME``.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be read from.
        layer_zoom (int): The zoom level of the layer that is to be read.
        options (dict, optional): Additional parameters for reading the layer for specific backends.
            The dictionary is only used for Cassandra and HBase, no other backend requires this
            to be set.
        numPartitions (int, optional): Sets RDD partition count when reading from catalog.
        **kwargs: The optional parameters can also be set as keywords arguments. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        :class:`~geopyspark.geotrellis.Metadata`
    """

    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(geopysc, uri, options)
    cached = _mapped_cached[uri]

    if rdd_type == SPATIAL:
        metadata = cached.store.metadataSpatial(layer_name, layer_zoom)
    else:
        metadata = cached.store.metadataSpaceTime(layer_name, layer_zoom)

    return Metadata.from_dict(json.loads(metadata))

def get_layer_ids(geopysc,
                  uri,
                  options=None,
                  **kwargs):
    """Returns a list of all of the layer ids in the selected catalog as dicts that contain the
    name and zoom of a given layer.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        options (dict, optional): Additional parameters for reading the layer for specific backends.
            The dictionary is only used for Cassandra and HBase, no other backend requires this
            to be set.
        **kwargs: The optional parameters can also be set as keywords arguments. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        [layerIds]

        Where ``layerIds`` is a ``dict`` with the following fields:
            - **name** (str): The name of the layer
            - **zoom** (int): The zoom level of the given layer.
    """

    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(geopysc, uri, options)
    cached = _mapped_cached[uri]

    return cached.reader.layerIds()

def read(geopysc,
         rdd_type,
         uri,
         layer_name,
         layer_zoom,
         options=None,
         numPartitions=None,
         **kwargs):

    """Reads a single, zoom layer from a GeoTrellis catalog.

    Please read more about the various backends that GeoPySpark supports and how to access them
    [link].

    Note:
        This will read the entire layer. If only part of the layer is needed,
        use :func:`query` instead.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: `SPATIAL` and `SPACETIME`.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be read from.
        layer_zoom (int): The zoom level of the layer that is to be read.
        options (dict, optional): Additional parameters for reading the layer for specific backends.
            The dictionary is only used for Cassandra and HBase, no other backend requires this
            to be set.
        numPartitions (int, optional): Sets RDD partition count when reading from catalog.
        **kwargs: The optional parameters can also be set as keywords arguments. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`

    """
    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(geopysc, uri, options)

    cached = _mapped_cached[uri]

    key = geopysc.map_key_input(rdd_type, True)

    if numPartitions is None:
        numPartitions  = geopysc.pysc.defaultMinPartitions

    srdd = cached.reader.read(key, layer_name, layer_zoom, numPartitions)

    return TiledRasterRDD(geopysc, rdd_type, srdd)

def read_value(geopysc,
               rdd_type,
               uri,
               layer_name,
               layer_zoom,
               col,
               row,
               zdt=None,
               options=None,
               **kwargs):

    """Reads a single tile from a GeoTrellis catalog.
    Unlike other functions in this module, this will not return a TiledRasterRDD, but rather a
    GeoPySpark formatted raster. This is the function to use when creating a tile server.

    Note:
        When requesting a tile that does not exist, ``None`` will be returned.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: `SPATIAL` and `SPACETIME`.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be read from.
        layer_zoom (int): The zoom level of the layer that is to be read.
        col (int): The col number of the tile within the layout. Cols run east to west.
        row (int): The row number of the tile within the layout. Row run north to south.
        zdt (str): The Zone-Date-Time string of the tile. The string must be in a valid date-time
            format. This parameter is only used when querying spatial-temporal data. The default
            value is, None. If None, then only the spatial area will be queried.
        options (dict, optional): Additional parameters for reading the tile for specific backends.
            The dictionary is only used for Cassandra and HBase, no other backend requires this
            to be set.
        **kwargs: The optional parameters can also be set as keywords arguments. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        :ref:`raster` or ``None``
    """

    if not _in_bounds(geopysc, rdd_type, uri, layer_name, layer_zoom, col, row):
        return None
    else:
        if options:
            options = options
        elif kwargs:
            options = kwargs
        else:
            options = {}

        _construct_catalog(geopysc, uri, options)
        cached = _mapped_cached[uri]

        if not zdt:
            zdt = ""

        key = geopysc.map_key_input(rdd_type, True)

        tup = cached.value_reader.readTile(key,
                                           layer_name,
                                           layer_zoom,
                                           col,
                                           row,
                                           zdt)

        ser = geopysc.create_value_serializer(tup._2(), TILE)

        if uri not in _mapped_serializers:
            _mapped_serializers[uri] = ser

        return ser.loads(tup._1())[0]

def query(geopysc,
          rdd_type,
          uri,
          layer_name,
          layer_zoom,
          intersects,
          time_intervals=None,
          proj_query=None,
          options=None,
          numPartitions=None,
          **kwargs):

    """Queries a single, zoom layer from a GeoTrellis catalog given spatial and/or time parameters.
    Unlike read, this method will only return part of the layer that intersects the specified
    region.

    Note:
        The whole layer could still be read in if `intersects` and/or `time_intervals` have not
        been set, or if the querried region contains the entire layer.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: SPATIAL and SPACETIME. Note: All of the
            GeoTiffs must have the same saptial type.
        uri (str): The Uniform Resource Identifier used to point towards the desired GeoTrellis
            catalog to be read from. The shape of this string varies depending on backend.
        layer_name (str): The name of the GeoTrellis catalog to be querried.
        layer_zoom (int): The zoom level of the layer that is to be querried.
        intersects (str or Polygon or :class:`~geopyspark.geotrellis.data_structures.Extent`): The
            desired spatial area to be returned. Can either be a string, a shapely Polygon, or an
            instance of ``Extent``. If the value is a string, it must be the WKT string, geometry
            format.

            The types of Polygons supported:
                * Point
                * Polygon
                * MultiPolygon

            Note:
                Only layers that were made from spatial, singleband GeoTiffs can query a Point.
                All other types are restricted to Polygon and MulitPolygon.
        time_intervals (list, optional): A list of strings that time intervals to query.
            The strings must be in a valid date-time format. This parameter is only used when
            querying spatial-temporal data. The default value is, None. If None, then only the
            spatial area will be querried.
        options (dict, optional): Additional parameters for querying the tile for specific backends.
            The dictioanry is only used for Cassandra and HBase, no other backend requires this
            to be set.
        numPartitions (int, optional): Sets RDD partition count when reading from catalog.
        **kwargs: The optional parameters can also be set as keywords arguements. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    Returns:
        :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`

    """
    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(geopysc, uri, options)

    cached = _mapped_cached[uri]

    key = geopysc.map_key_input(rdd_type, True)

    if time_intervals is None:
        time_intervals = []

    if proj_query is None:
        proj_query = ""
    if isinstance(proj_query, int):
        proj_query = "EPSG:" + str(proj_query)

    if numPartitions is None:
        numPartitions  = geopysc.pysc.defaultMinPartitions

    if isinstance(intersects, Polygon) or isinstance(intersects, MultiPolygon) \
       or isinstance(intersects, Point):
        srdd = cached.reader.query(key,
                                   layer_name,
                                   layer_zoom,
                                   dumps(intersects),
                                   time_intervals,
                                   proj_query,
                                   numPartitions)

    elif isinstance(intersects, Extent):
        srdd = cached.reader.query(key,
                                   layer_name,
                                   layer_zoom,
                                   dumps(intersects.to_poly),
                                   time_intervals,
                                   proj_query,
                                   numPartitions)

    elif isinstance(intersects, str):
        srdd = cached.reader.query(key,
                                   layer_name,
                                   layer_zoom,
                                   intersects,
                                   time_intervals,
                                   proj_query)
    else:
        raise TypeError("Could not query intersection", intersects)

    return TiledRasterRDD(geopysc, rdd_type, srdd)

def write(uri,
          layer_name,
          tiled_raster_rdd,
          index_strategy=ZORDER,
          time_unit=None,
          options=None,
          **kwargs):

    """Writes a tile layer to a specified destination.

    Args:
        uri (str): The Uniform Resource Identifier used to point towards the desired location for
            the tile layer to written to. The shape of this string varies depending on backend.
        layer_name (str): The name of the new, tile layer.
        layer_zoom (int): The zoom level the layer should be saved at.
        tiled_raster_rdd (TiledRasterRDD): The TiledRasterRDD to be saved.
        index_strategy (str): The method used to orginize the saved data. Depending on the type of
            data within the layer, only certain methods are available. The default method used is,
            `ZORDER`.
        time_unit (str, optional): Which time unit should be used when saving spatial-temporal data.
            While this is set to None as default, it must be set if saving spatial-temporal data.
            Depending on the indexing method chosen, different time units are used.
        options (dict, optional): Additional parameters for writing the layer for specific
            backends. The dictioanry is only used for Cassandra and HBase, no other backend
            requires this to be set.
        **kwargs: The optional parameters can also be set as keywords arguements. The keywords must
            be in camel case. If both options and keywords are set, then the options will be used.

    """
    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(tiled_raster_rdd.geopysc, uri, options)

    cached = _mapped_cached[uri]

    if not time_unit:
        time_unit = ""

    if tiled_raster_rdd.rdd_type == SPATIAL:
        cached.writer.writeSpatial(layer_name,
                                   tiled_raster_rdd.srdd,
                                   index_strategy)
    else:
        cached.writer.writeTemporal(layer_name,
                                    tiled_raster_rdd.srdd,
                                    time_unit,
                                    index_strategy)
