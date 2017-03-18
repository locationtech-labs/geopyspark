import json

from geopyspark.geotrellis.constants import SPATIAL, TILE, ZORDER
from shapely.geometry import Polygon
from shapely.wkt import dumps
from urllib.parse import urlparse


_mapped_builds = {}


def _construct_catalog(geopysc, new_uri, options):
    if new_uri not in _mapped_builds:

        parsed_uri = urlparse(new_uri)
        backend = parsed_uri.scheme

        if backend == 'hdfs':
            store = geopysc.store_factory.buildHadoop(new_uri)
            reader = geopysc.reader_factory.buildHadoop(store, geopysc.sc)
            writer = geopysc.writer_factory.buildHadoop(store)

        elif backend == 'file':
            store = geopysc.store_factory.buildFile(new_uri)
            reader = geopysc.reader_factory.buildFile(store, geopysc.sc)
            writer = geopysc.writer_factory.buildFile(store)

        elif backend == 's3':
            store = geopysc.store_factory.buildS3(parsed_uri.netloc, parsed_uri.path[1:])
            reader = geopysc.reader_factory.buildS3(store, geopysc.sc)
            writer = geopysc.writer_factory.buildS3(store)

        elif backend == 'cassandra':
            parameters = parsed_uri.query.split('&')
            parameter_dict = {}

            for param in parameters:
                split_param = param.split('=', 1)
                parameter_dict[split_param[0]] = split_param[1]

            store = geopysc.store_factory.buildCassandra(
                parameter_dict['host'],
                parameter_dict['username'],
                parameter_dict['password'],
                parameter_dict['keyspace'],
                parameter_dict['table'],
                options)

            reader = geopysc.reader_factory.buildCassandra(store, geopysc.sc)
            writer = geopysc.writer_factory.buildCassandra(store,
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

            store = geopysc.store_factory.buildHBase(zookeepers, master, port, table)
            reader = geopysc.reader_factory.buildHBase(store, geopysc.sc)
            writer = geopysc.writer_factory.buildHBase(store, table)

        elif backend == 'accumulo':

            # The assumed uri looks like: accumulo//username:password/zoo1, zoo2/instance/table
            (user, password) = parsed_uri.netloc.split(':')
            split_parameters = parsed_uri.path.split('/')[1:]

            store = geopysc.store_factory.buildAccumulo(split_parameters[0],
                                                        split_parameters[1],
                                                        user,
                                                        password,
                                                        split_parameters[2])

            reader = geopysc.reader_factory.buildAccumulo(split_parameters[1],
                                                          store,
                                                          geopysc.sc)

            writer = geopysc.writer_factory.buildAccumulo(split_parameters[1],
                                                          store,
                                                          split_parameters[2])

        else:
            raise Exception("Cannot find Attribute Store for, {}".format(backend))

        _mapped_builds[new_uri] = (store, reader, writer)

def read(geopysc,
         rdd_type,
         uri,
         layer_name,
         layer_zoom,
         options=None,
         **kwargs):

    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(geopysc, uri, options)

    (store, reader, _) = _mapped_builds[uri]

    key = geopysc.map_key_input(rdd_type, True)

    tup = reader.read(key, layer_name, layer_zoom)
    schema = tup._2()

    if rdd_type == SPATIAL:
        jmetadata = store.metadataSpatial(layer_name, layer_zoom)
    else:
        jmetadata = store.metadataSpaceTime(layer_name, layer_zoom)

    metadata = json.loads(jmetadata)
    ser = geopysc.create_tuple_serializer(schema, value_type=TILE)

    rdd = geopysc.create_python_rdd(tup._1(), ser)

    return (rdd, schema, metadata)

def query(geopysc,
          rdd_type,
          uri,
          layer_name,
          layer_zoom,
          intersects,
          time_intervals=None,
          options=None,
          **kwargs):

    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(geopysc, uri, options)

    (store, reader, _) = _mapped_builds[uri]

    key = geopysc.map_key_input(rdd_type, True)

    if time_intervals is None:
        time_intervals = []

    if isinstance(intersects, Polygon):
        tup = reader.query(key,
                           layer_name,
                           layer_zoom,
                           dumps(intersects),
                           time_intervals)

    elif isinstance(intersects, str):
        tup = reader.query(key,
                           layer_name,
                           layer_zoom,
                           intersects,
                           time_intervals)
    else:
        raise Exception("Could not query intersection", intersects)

    schema = tup._2()

    if rdd_type == SPATIAL:
        jmetadata = store.metadataSpatial(layer_name, layer_zoom)
    else:
        jmetadata = store.metadataSpaceTime(layer_name, layer_zoom)

    metadata = json.loads(jmetadata)
    ser = geopysc.create_tuple_serializer(schema, value_type=TILE)

    rdd = geopysc.create_python_rdd(tup._1(), ser)

    return (rdd, schema, metadata)

def write(geopysc,
          rdd_type,
          uri,
          layer_name,
          layer_zoom,
          rdd,
          metadata,
          index_strategy=ZORDER,
          time_unit=None,
          options=None,
          **kwargs):

    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    _construct_catalog(geopysc, uri, options)

    (_, _, writer) = _mapped_builds[uri]

    key = geopysc.map_key_input(rdd_type, True)

    schema = metadata.schema

    if not time_unit:
        time_unit = ""

    writer.write(key,
                 layer_name,
                 layer_zoom,
                 rdd._jrdd,
                 json.dumps(metadata),
                 schema,
                 time_unit,
                 index_strategy)
