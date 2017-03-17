import json
import re

from shapely.geometry import Polygon
from shapely.wkt import dumps
from urllib.parse import urlparse


class Catalog(object):
    def __init__(self, geopysc, options=None, **kwargs):
        self.geopysc = geopysc

        self.uri = None

        self.store = None
        self.reader = None
        self.writer = None

        if options:
            self.options = options
        elif kwargs:
            self.options = kwargs
        else:
            self.options = {}

    def _construct_catalog(self, new_uri):
        if new_uri != self.uri and new_uri is not None:
            self.uri = new_uri

            parsed_uri = urlparse(self.uri)
            backend = parsed_uri.scheme

            if backend == 'hdfs':
                self.store = self.geopysc.store_factory.buildHadoop(self.uri)

                self.reader = \
                        self.geopysc.reader_factory.buildHadoop(self.store,
                                                                self.geopysc.sc)
                self.writer = \
                        self.geopysc.writer_factory.buildHadoop(self.store)

            elif backend == 'file':
                self.store = self.geopysc.store_factory.buildFile(self.uri)

                self.reader = \
                        self.geopysc.reader_factory.buildFile(self.store,
                                                              self.geopysc.sc)
                self.writer = \
                        self.geopysc.writer_factory.buildFile(self.store)

            elif backend == 's3':
                self.store = self.geopysc.store_factory.buildS3(parsed_uri.netloc,
                                                                parsed_uri.path[1:])

                self.reader = \
                        self.geopysc.reader_factory.buildS3(self.store,
                                                            self.geopysc.sc)

                self.writer = self.geopysc.writer_factory.buildS3(self.store)

            elif backend == 'cassandra':
                parameters = parsed_uri.query.split('&')
                parameter_dict = {}

                for param in parameters:
                    split_param = param.split('=', 1)
                    parameter_dict[split_param[0]] = split_param[1]

                self.store = self.geopysc.store_factory.buildCassandra(
                    parameter_dict['host'],
                    parameter_dict['username'],
                    parameter_dict['password'],
                    parameter_dict['keyspace'],
                    parameter_dict['table'],
                    self.options)

                self.reader = \
                        self.geopysc.reader_factory.buildCassandra(self.store,
                                                                   self.geopysc.sc)

                self.writer = \
                        self.geopysc.writer_factory.buildCassandra(self.store,
                                                                   parameter_dict['keyspace'],
                                                                   parameter_dict['table'])

            elif backend == 'hbase':

                # The assumed uri looks like: hbase://zoo1, zoo2, ..., zooN: port/table
                (zookeepers, port) = parsed_uri.netloc.split(':')
                table = parsed_uri.path

                if 'master' in self.options:
                    master = self.options['master']
                else:
                    master = ""


                self.store = \
                        self.geopysc.store_factory.buildHBase(zookeepers,
                                                              master,
                                                              port,
                                                              table)
                self.reader = \
                        self.geopysc.reader_factory.buildHBase(self.store,
                                                               self.geopysc.sc)

                self.writer = \
                        self.geopysc.writer_factory.buildHBase(self.store,
                                                               table)

            elif backend == 'accumulo':

                # The assumed uri looks like: accumulo//username:password/zoo1, zoo2/instance/table
                (user, password) = parsed_uri.netloc.split(':')
                split_parameters = parsed_uri.path.split('/')[1:]

                self.store = \
                        self.geopysc.store_factory.buildAccumulo(split_parameters[0],
                                                                 split_parameters[1],
                                                                 user,
                                                                 password,
                                                                 split_parameters[2])
                self.reader = \
                        self.geopysc.reader_factory.buildAccumulo(split_parameters[1],
                                                                  self.store,
                                                                  self.geopysc.sc)
                self.writer = \
                        self.geopysc.writer_factory.buildAccumulo(split_parameters[1],
                                                                  self.store,
                                                                  split_parameters[2])

            else:
                raise Exception("Cannot find Attribute Store for, {}".format(backend))

        def read(self,
                 rdd_type,
                 uri,
                 layer_name,
                 layer_zoom):

            self._construct_catalog(uri)

            key = self.geopysc.map_key_input(rdd_type, True)

            tup = self.reader.read(key, layer_name, layer_zoom)
            schema = tup._2()

            if rdd_type == "spatial":
                jmetadata = self.store.metadataSpatial(layer_name, layer_zoom)
            else:
                jmetadata = self.store.metadataSpaceTime(layer_name, layer_zoom)

            metadata = json.loads(jmetadata)
            ser = self.geopysc.create_tuple_serializer(schema, value_type="Tile")

            rdd = self.geopysc.create_python_rdd(tup._1(), ser)

            return (rdd, schema, metadata)

        def query(self,
                  rdd_type,
                  uri,
                  layer_name,
                  layer_zoom,
                  intersects,
                  time_intervals=None):

            self._construct_catalog(uri)

            key = self.geopysc.map_key_input(rdd_type, True)

            if time_intervals is None:
                time_intervals = []

            if isinstance(intersects, Polygon):
                tup = self.reader.query(key,
                                        layer_name,
                                        layer_zoom,
                                        dumps(intersects),
                                        time_intervals)

            elif isinstance(intersects, str):
                tup = self.reader.query(key,
                                        layer_name,
                                        layer_zoom,
                                        intersects,
                                        time_intervals)
            else:
                raise Exception("Could not query intersection", intersects)

            schema = tup._2()

            if rdd_type == "spatial":
                jmetadata = self.store.metadataSpatial(layer_name, layer_zoom)
            else:
                jmetadata = self.store.metadataSpaceTime(layer_name, layer_zoom)

            metadata = json.loads(jmetadata)
            ser = self.geopysc.create_tuple_serializer(schema, value_type="Tile")

            rdd = self.geopysc.create_python_rdd(tup._1(), ser)

            return (rdd, schema, metadata)

        def write(self,
                  rdd_type,
                  layer_name,
                  layer_zoom,
                  rdd,
                  metadata,
                  index_strategy="zorder",
                  time_unit=None,
                  uri=None):

            if uri is not None:
                self._construct_catalog(uri)

            key = self.geopysc.map_key_input(rdd_type, True)

            schema = metadata.schema

            if not time_unit:
                time_unit = ""

            self.writer.write(key,
                              layer_name,
                              layer_zoom,
                              rdd._jrdd,
                              json.dumps(metadata),
                              schema,
                              time_unit,
                              index_strategy)
