from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.avroserializer import AvroSerializer

from shapely.geometry import Polygon
from shapely.wkt import dumps


class Metadata(object):
    def __init__(self, key_type, value_type, schema, jmetadata):
        self.key_type = key_type
        self.value_type = value_type
        self.schema = schema
        self.jmetadata = jmetadata


class _Catalog(object):

    def __init__(self, pysc, avroregistry):
        self.pysc = pysc
        self.avroregistry = avroregistry

        self.store = None
        self.reader = None
        self.writer = None

    @property
    def store_factory(self):
        return self.pysc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory

    @property
    def reader_factory(self):
        return self.pysc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory

    @property
    def writer_factory(self):
        return self.pysc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory

    def _get_spatial_jmetadata(self, layer_name, layer_zoom):
        return self.store.metadataSpatial(layer_name, layer_zoom)

    def _get_spacetime_jmetadata(self, layer_name, layer_zoom):
        return self.store.metadataSpaceTime(layer_name, layer_zoom)

    def _construct_return(self, java_rdd, schema, metadata):
        serializer = AvroSerializer(schema, self.avroregistry)
        rdd = RDD(java_rdd, self.pysc, AutoBatchedSerializer(serializer))

        return (rdd, metadata)

    def _read_fully(self,
                    key_type,
                    value_type,
                    reader_method,
                    layer_name,
                    layer_zoom):

        tup = reader_method(layer_name, layer_zoom)
        schema = tup._2()

        if key_type == "spatial":
            jmetadata = self._get_spatial_jmetadata(layer_name, layer_zoom)
        else:
            jmetadata = self._get_spacetime_jmetadata(layer_name, layer_zoom)

        metadata = Metadata(key_type, value_type, schema, jmetadata)

        return self._construct_return(tup._1(), schema, metadata)

    def _query_spatial(self,
                       reader_method,
                       value_type,
                       layer_name,
                       layer_zoom,
                       intersects):

        if isinstance(intersects, Polygon):
            tup = reader_method(layer_name, layer_zoom, dumps(intersects))
        elif isinstance(intersects, str):
            tup = reader_method(layer_name, layer_zoom, intersects)
        else:
            raise BaseException("Bad polygon")

        schema = tup._2()

        jmetadata = self._get_spatial_jmetadata(layer_name, layer_zoom)
        metadata = Metadata("spatial", value_type, schema, jmetadata)

        return self._construct_return(tup._1(), schema, metadata)

    def _query_spacetime(self,
                         reader_method,
                         value_type,
                         layer_name,
                         layer_zoom,
                         intersects,
                         time_intervals):

        if isinstance(intersects, Polygon):
            tup = reader_method(layer_name,
                                layer_zoom,
                                dumps(intersects),
                                time_intervals)

        elif isinstance(intersects, str):
            tup = reader_method(layer_name,
                                layer_zoom,
                                intersects,
                                time_intervals)
        else:
            raise BaseException("Bad polygon")

        schema = tup._2()

        jmetadata = self._get_spacetime_jmetadata(layer_name, layer_zoom)
        metadata = Metadata("spacetime", value_type, schema, jmetadata)

        return self._construct_return(tup._1(), schema, metadata)

    def _write_spatial(self,
                       writer_method,
                       layer_name,
                       layer_zoom,
                       rdd,
                       metadata,
                       index_strategy):

        schema = metadata.schema
        jmetadata = metadata.jmetadata

        writer_method(layer_name,
                      layer_zoom,
                      rdd._jrdd,
                      jmetadata,
                      schema,
                      index_strategy)

    def _write_spacetime(self,
                         writer_method,
                         layer_name,
                         layer_zoom,
                         rdd,
                         metadata,
                         time_unit,
                         index_strategy):

        schema = metadata.schema
        jmetadata = metadata.jmetadata

        if not time_unit:
            time_unit = ""

        writer_method(layer_name,
                      layer_zoom,
                      rdd._jrdd,
                      jmetadata,
                      schema,
                      time_unit,
                      index_strategy)

    def _query_spatial_singleband(self,
                                  layer_name,
                                  layer_zoom,
                                  intersects):
        if intersects is None:
            reader_method = self.reader.readSpatialSingleband
            return self._read_fully("spatial",
                                    "singleband",
                                    reader_method,
                                    layer_name,
                                    layer_zoom)
        else:
            reader_method = self.reader.querySpatialSingleband
            return self._query_spatial(reader_method,
                                       "singleband",
                                       layer_name,
                                       layer_zoom,
                                       intersects)

    def _query_spatial_multiband(self,
                                 layer_name,
                                 layer_zoom,
                                 intersects):

        if intersects is None:
            reader_method = self.reader.readSpatialMultiband
            return self._read_fully("spatial",
                                    "multiband",
                                    reader_method,
                                    layer_name,
                                    layer_zoom)
        else:
            reader_method = self.reader.querySpatialMultiband
            return self._query_spatial(reader_method,
                                       "multiband",
                                       layer_name,
                                       layer_zoom,
                                       intersects)

    def _query_spacetime_singleband(self,
                                    layer_name,
                                    layer_zoom,
                                    intersects,
                                    time_intervals):
        if intersects is None:
            reader_method = self.reader.readSpaceTimeSingleband
            return self._read_fully("spacetime",
                                    "singleband",
                                    reader_method,
                                    layer_name,
                                    layer_zoom)
        else:
            reader_method = self.reader.querySpaceTimeSingleband
            return self._query_spacetime(reader_method,
                                         "singleband",
                                         layer_name,
                                         layer_zoom,
                                         intersects,
                                         time_intervals)

    def _query_spacetime_multiband(self,
                                   layer_name,
                                   layer_zoom,
                                   intersects,
                                   time_intervals):

        if intersects is None:
            reader_method = self.reader.readSpaceTimeMultiband
            return self._read_fully("spacetime",
                                    "multiband",
                                    reader_method,
                                    layer_name,
                                    layer_zoom)
        else:
            reader_method = self.reader.querySpaceTimeMultiband
            return self._query_spacetime(reader_method,
                                         "multiband",
                                         layer_name,
                                         layer_zoom,
                                         intersects,
                                         time_intervals)

    def _write_spatial_singleband(self,
                                  layer_name,
                                  layer_zoom,
                                  rdd,
                                  metadata,
                                  index_strategy):

        writer_method = self.writer.writeSpatialSingleband
        self._write_spatial(writer_method,
                            layer_name,
                            layer_zoom,
                            rdd,
                            metadata,
                            index_strategy)

    def _write_spatial_multiband(self,
                                 layer_name,
                                 layer_zoom,
                                 rdd,
                                 metadata,
                                 index_strategy):

        writer_method = self.writer.writeSpatialMultiband
        self._write_spatial(writer_method,
                            layer_name,
                            layer_zoom,
                            rdd,
                            metadata,
                            index_strategy)

    def _write_spacetime_singleband(self,
                                    layer_name,
                                    layer_zoom,
                                    rdd,
                                    metadata,
                                    time_unit,
                                    index_strategy):

        writer_method = self.writer.writeSpaceTimeSingleband
        self._write_spacetime(writer_method,
                              layer_name,
                              layer_zoom,
                              rdd,
                              metadata,
                              time_unit,
                              index_strategy)

    def _write_spacetime_multiband(self,
                                   layer_name,
                                   layer_zoom,
                                   rdd,
                                   metadata,
                                   time_unit,
                                   index_strategy):

        writer_method = self.writer.writeSpaceTimeMultiband
        self._write_spacetime(writer_method,
                              layer_name,
                              layer_zoom,
                              rdd,
                              metadata,
                              time_unit,
                              index_strategy)


class HadoopCatalog(_Catalog):

    __slots__ = ["pysc",
                 "avrogregistry",
                 "store",
                 "reader",
                 "writer",
                 "uri"]

    def __init__(self, pysc, avroregistry=None):

        super().__init__(pysc, avroregistry)
        self.pysc = pysc
        self._sc = pysc._jsc.sc()

        self.avroregistry = avroregistry

        self.uri = None

    def _construct_catalog(self, new_uri):
        if new_uri != self.uri and new_uri is not None:
            self.uri = new_uri

            self.store = self.store_factory.buildHadoop(self.uri)

            self._construct_reader()
            self._construct_writer()

    def _construct_reader(self):
        self.reader = \
                self.reader_factory.buildHadoop(self.store, self._sc)

    def _construct_writer(self):
        self.writer = \
                self.writer_factory.buildHadoop(self.store)

    def query_spatial_singleband(self,
                                 uri,
                                 layer_name,
                                 layer_zoom,
                                 intersects=None):

        self._construct_catalog(uri)

        return self._query_spatial_singleband(layer_name,
                                              layer_zoom,
                                              intersects)

    def query_spatial_multiband(self,
                                uri,
                                layer_name,
                                layer_zoom,
                                intersects=None):

        self._construct_catalog(uri)

        return self._query_spatial_multiband(layer_name,
                                             layer_zoom,
                                             intersects)

    def query_spacetime_singleband(self,
                                   uri,
                                   layer_name,
                                   layer_zoom,
                                   intersects=None,
                                   time_intervals=[]):

        self._construct_catalog(uri)

        return self._query_spacetime_singleband(layer_name,
                                                layer_zoom,
                                                intersects,
                                                time_intervals)

    def query_spacetime_multiband(self,
                                  uri,
                                  layer_name,
                                  layer_zoom,
                                  intersects=None,
                                  time_intervals=[]):

        self._construct_catalog(uri)

        return self._query_spacetime_multiband(layer_name,
                                               layer_zoom,
                                               intersects,
                                               time_intervals)

    def write_spatial_singleband(self,
                                 layer_name,
                                 layer_zoom,
                                 rdd,
                                 metadata,
                                 index_strategy="zorder",
                                 uri=None):

        if uri is not None:
            self._construct_catalog(uri)

        self._write_spatial_singleband(layer_name,
                                       layer_zoom,
                                       rdd,
                                       metadata,
                                       index_strategy)

    def write_spatial_multiband(self,
                                layer_name,
                                layer_zoom,
                                rdd,
                                metadata,
                                index_strategy="zorder",
                                uri=None):

        if uri is not None:
            self._construct_catalog(uri)

        self._write_spatial_multiband(layer_name,
                                      layer_zoom,
                                      rdd,
                                      metadata,
                                      index_strategy)

    def write_spacetime_singleband(self,
                                   layer_name,
                                   layer_zoom,
                                   rdd,
                                   metadata,
                                   time_unit=None,
                                   index_strategy="zorder",
                                   uri=None):

        if uri is not None:
            self._construct_catalog(uri)

        self._write_spacetime_singleband(layer_name,
                                         layer_zoom,
                                         rdd,
                                         metadata,
                                         time_unit,
                                         index_strategy)

    def write_spacetime_multiband(self,
                                  layer_name,
                                  layer_zoom,
                                  rdd,
                                  metadata,
                                  time_unit=None,
                                  index_strategy="zorder",
                                  uri=None):

        if uri is not None:
            self._construct_catalog(uri)

        self._write_spacetime_multiband(layer_name,
                                        layer_zoom,
                                        rdd,
                                        metadata,
                                        time_unit,
                                        index_strategy)


class S3Catalog(_Catalog):

    __slots__ = ["pysc",
                 "avrogregistry",
                 "store",
                 "reader",
                 "writer",
                 "bucket",
                 "prefix"]

    def __init__(self, pysc, avroregistry=None):

        super().__init__(pysc, avroregistry)
        self.pysc = pysc
        self._sc = pysc._jsc.sc()

        self.avroregistry = avroregistry

        self.bucket = None
        self.prefix = None

    def _construct_catalog(self, new_bucket, new_prefix):
        bucket = new_bucket != self.bucket and new_bucket is not None
        prefix = new_prefix != self.prefix and new_prefix is not None

        if bucket or prefix:
            self.bucket = new_bucket
            self.prefix = new_prefix

            self.store = self.store_factory.buildS3(self.bucket,
                                                    self.prefix)

            self._construct_reader()
            self._construct_writer()

    def _construct_reader(self):
        self.reader = \
                self.reader_factory.buildS3(self.store, self._sc)

    def _construct_writer(self):
        self.writer = self.writer_factory.buildS3(self.store)

    def query_spatial_singleband(self,
                                 bucket,
                                 prefix,
                                 layer_name,
                                 layer_zoom,
                                 intersects=None):

        self._construct_catalog(bucket, prefix)

        return self._query_spatial_singleband(layer_name,
                                              layer_zoom,
                                              intersects)

    def query_spatial_multiband(self,
                                bucket,
                                prefix,
                                layer_name,
                                layer_zoom,
                                intersects=None):

        self._construct_catalog(bucket, prefix)

        return self._query_spatial_multiband(layer_name,
                                             layer_zoom,
                                             intersects)

    def query_spacetime_singleband(self,
                                   bucket,
                                   prefix,
                                   layer_name,
                                   layer_zoom,
                                   intersects=None,
                                   time_intervals=[]):

        self._construct_catalog(bucket, prefix)

        return self._query_spacetime_singleband(layer_name,
                                                layer_zoom,
                                                intersects,
                                                time_intervals)

    def query_spacetime_multiband(self,
                                  bucket,
                                  prefix,
                                  layer_name,
                                  layer_zoom,
                                  intersects=None,
                                  time_intervals=[]):

        self._construct_catalog(bucket, prefix)

        return self._query_spacetime_multiband(layer_name,
                                               layer_zoom,
                                               intersects,
                                               time_intervals)

    def write_spatial_singleband(self,
                                 layer_name,
                                 layer_zoom,
                                 rdd,
                                 metadata,
                                 index_strategy="zorder",
                                 bucket=None,
                                 prefix=None):

        if bucket is not None or prefix is not None:
            self._construct_catalog(bucket, prefix)

        self._write_spatial_singleband(layer_name,
                                       layer_zoom,
                                       rdd,
                                       metadata,
                                       index_strategy)

    def write_spatial_multiband(self,
                                layer_name,
                                layer_zoom,
                                rdd,
                                metadata,
                                index_strategy="zorder",
                                bucket=None,
                                prefix=None):

        if bucket is not None or prefix is not None:
            self._construct_catalog(bucket, prefix)

        self._write_spatial_multiband(layer_name,
                                      layer_zoom,
                                      rdd,
                                      metadata,
                                      index_strategy)

    def write_spacetime_singleband(self,
                                   layer_name,
                                   layer_zoom,
                                   rdd,
                                   metadata,
                                   time_unit=None,
                                   index_strategy="zorder",
                                   bucket=None,
                                   prefix=None):

        if bucket is not None or prefix is not None:
            self._construct_catalog(bucket, prefix)

        self._write_spacetime_singleband(layer_name,
                                         layer_zoom,
                                         rdd,
                                         metadata,
                                         time_unit,
                                         index_strategy)

    def write_spacetime_multiband(self,
                                  layer_name,
                                  layer_zoom,
                                  rdd,
                                  metadata,
                                  time_unit=None,
                                  index_strategy="zorder",
                                  bucket=None,
                                  prefix=None):

        if bucket is not None or prefix is not None:
            self._construct_catalog(bucket, prefix)

        self._write_spacetime_singleband(layer_name,
                                         layer_zoom,
                                         rdd,
                                         metadata,
                                         time_unit,
                                         index_strategy)


class FileCatalog(_Catalog):

    __slots__ = ["pysc",
                 "avrogregistry",
                 "store",
                 "reader",
                 "writer",
                 "path"]

    def __init__(self, pysc, avroregistry=None):

        super().__init__(pysc, avroregistry)
        self.pysc = pysc
        self._sc = pysc._jsc.sc()

        self.avroregistry = avroregistry

        self.path = None

    def _construct_catalog(self, new_path):
        if new_path != self.path and new_path is not None:
            self.path = new_path

            self.store = self.store_factory.buildFile(self.path)

            self._construct_reader()
            self._construct_writer()

    def _construct_reader(self):
        self.reader = \
                self.reader_factory.buildFile(self.store, self._sc)

    def _construct_writer(self):
        self.writer = \
                self.writer_factory.buildFile(self.store)

    def query_spatial_singleband(self,
                                 path,
                                 layer_name,
                                 layer_zoom,
                                 intersects=None):

        self._construct_catalog(path)

        return self._query_spatial_singleband(layer_name,
                                              layer_zoom,
                                              intersects)

    def query_spatial_multiband(self,
                                path,
                                layer_name,
                                layer_zoom,
                                intersects=None):

        self._construct_catalog(path)

        return self._query_spatial_multiband(layer_name,
                                             layer_zoom,
                                             intersects)

    def query_spacetime_singleband(self,
                                   path,
                                   layer_name,
                                   layer_zoom,
                                   intersects=None,
                                   time_intervals=[]):

        self._construct_catalog(path)

        return self._query_spacetime_singleband(layer_name,
                                                layer_zoom,
                                                intersects,
                                                time_intervals)

    def query_spacetime_multiband(self,
                                  path,
                                  layer_name,
                                  layer_zoom,
                                  intersects=None,
                                  time_intervals=[]):

        self._construct_catalog(path)

        return self._query_spacetime_multiband(layer_name,
                                               layer_zoom,
                                               intersects,
                                               time_intervals)

    def write_spatial_singleband(self,
                                 layer_name,
                                 layer_zoom,
                                 rdd,
                                 metadata,
                                 index_strategy="zorder",
                                 path=None):

        if path is not None:
            self._construct_catalog(path)

        self._write_spatial_singleband(layer_name,
                                       layer_zoom,
                                       rdd,
                                       metadata,
                                       index_strategy)

    def write_spatial_multiband(self,
                                layer_name,
                                layer_zoom,
                                rdd,
                                metadata,
                                index_strategy="zorder",
                                path=None):

        if path is not None:
            self._construct_catalog(path)

        self._write_spatial_multiband(layer_name,
                                      layer_zoom,
                                      rdd,
                                      metadata,
                                      index_strategy)

    def write_spacetime_singleband(self,
                                   layer_name,
                                   layer_zoom,
                                   rdd,
                                   metadata,
                                   time_unit=None,
                                   index_strategy="zorder",
                                   path=None):

        if path is not None:
            self._construct_catalog(path)

        self._write_spacetime_singleband(layer_name,
                                         layer_zoom,
                                         rdd,
                                         metadata,
                                         time_unit,
                                         index_strategy)

    def write_spacetime_multiband(self,
                                  layer_name,
                                  layer_zoom,
                                  rdd,
                                  metadata,
                                  time_unit=None,
                                  index_strategy="zorder",
                                  path=None):

        if path is not None:
            self._construct_catalog(path)

        self._write_spacetime_multiband(layer_name,
                                        layer_zoom,
                                        rdd,
                                        metadata,
                                        time_unit,
                                        index_strategy)


class CassandraCatalog(_Catalog):

    __slots__ = ["pysc",
                 "avrogregistry",
                 "store",
                 "reader",
                 "writer",
                 "hosts",
                 "username",
                 "password",
                 "replication_strategy",
                 "replication_factor",
                 "local_dc",
                 "uhprd",
                 "allow_remote_dcs_for_lcl",
                 "attribute_key_space",
                 "attribute_table"]

    def __init__(self,
                 pysc,
                 hosts,
                 username,
                 password,
                 replication_strategy,
                 replication_factor,
                 local_dc,
                 uhprd,
                 allow_remote_dcs_for_lcl,
                 avroregistry=None):

        super().__init__(pysc, avroregistry)
        self.pysc = pysc
        self._sc = self.pysc._jsc.sc()

        self.hosts = hosts
        self.username = username
        self.password = password
        self.replication_strategy = replication_strategy
        self.replication_factor = replication_factor
        self.local_dc = local_dc
        self.uhpd = uhprd
        self.allow_remote_dcs_for_lcl = allow_remote_dcs_for_lcl
        self.avroregistry = avroregistry

        self.attribute_key_space = None
        self.attribute_table = None

    def _construct_catalog(self, new_attribute_key_space, new_attribute_table):
        is_old_key = new_attribute_key_space != self.attribute_key_space
        is_none_key = new_attribute_key_space is not None

        is_old_table = new_attribute_table != self.attribute_table
        is_none_table = new_attribute_table is not None

        if (is_old_key and is_none_key) or (is_old_table and is_none_table):
            self.attribute_key_space = new_attribute_key_space
            self.attribute_table = new_attribute_table

            self.store = self.store_factory.buildCassandra(
                self.hosts,
                self.username,
                self.password,
                self.replication_strategy,
                self.replication_factor,
                self.local_dc,
                self.uhpd,
                self.allow_remote_dcs_for_lcl,
                self.attribute_key_space,
                self.attribute_table)

            self._construct_reader()
            self._construct_writer()

    def _construct_reader(self):
        self.reader = \
                self.reader_factory.buildCassandra(self.store,
                                                   self._sc)

    def _construct_writer(self):
        self.writer = \
                self.writer_factory.buildCassandra(self.store,
                                                   self.attribute_key_space,
                                                   self.attribute_table)

    def query_spatial_singleband(self,
                                 attribute_key_space,
                                 attribute_table,
                                 layer_name,
                                 layer_zoom,
                                 intersects=None):

        self._construct_catalog(attribute_key_space, attribute_table)

        return self._query_spatial_singleband(layer_name,
                                              layer_zoom,
                                              intersects)

    def query_spatial_multiband(self,
                                attribute_key_space,
                                attribute_table,
                                layer_name,
                                layer_zoom,
                                intersects=None):

        self._construct_catalog(attribute_key_space, attribute_table)

        return self._query_spatial_multiband(layer_name,
                                             layer_zoom,
                                             intersects)

    def query_spacetime_singleband(self,
                                   attribute_key_space,
                                   attribute_table,
                                   layer_name,
                                   layer_zoom,
                                   intersects=None,
                                   time_intervals=[]):

        self._construct_catalog(attribute_key_space, attribute_table)

        return self._query_spacetime_singleband(layer_name,
                                                layer_zoom,
                                                intersects,
                                                time_intervals)

    def query_spacetime_multiband(self,
                                  attribute_key_space,
                                  attribute_table,
                                  layer_name,
                                  layer_zoom,
                                  intersects=None,
                                  time_intervals=[]):

        self._construct_catalog(attribute_key_space, attribute_table)

        return self._query_spacetime_multiband(layer_name,
                                               layer_zoom,
                                               intersects,
                                               time_intervals)

    def write_spatial_singleband(self,
                                 layer_name,
                                 layer_zoom,
                                 rdd,
                                 metadata,
                                 index_strategy="zorder",
                                 attribute_key_space=None,
                                 attribute_table=None):

        if attribute_key_space is not None or attribute_table is not None:
            self._construct_catalog(attribute_key_space, attribute_table)

        self._write_spatial_singleband(layer_name,
                                       layer_zoom,
                                       rdd,
                                       metadata,
                                       index_strategy)

    def write_spatial_multiband(self,
                                layer_name,
                                layer_zoom,
                                rdd,
                                metadata,
                                index_strategy="zorder",
                                attribute_key_space=None,
                                attribute_table=None):

        if attribute_key_space is not None or attribute_table is not None:
            self._construct_catalog(attribute_key_space, attribute_table)

        self._write_spatial_multiband(layer_name,
                                      layer_zoom,
                                      rdd,
                                      metadata,
                                      index_strategy)

    def write_spacetime_singleband(self,
                                   layer_name,
                                   layer_zoom,
                                   rdd,
                                   metadata,
                                   time_unit=None,
                                   index_strategy="zorder",
                                   attribute_key_space=None,
                                   attribute_table=None):

        if attribute_key_space is not None or attribute_table is not None:
            self._construct_catalog(attribute_key_space, attribute_table)

        self._write_spacetime_singleband(layer_name,
                                         layer_zoom,
                                         rdd,
                                         metadata,
                                         time_unit,
                                         index_strategy)

    def write_spacetime_multiband(self,
                                  layer_name,
                                  layer_zoom,
                                  rdd,
                                  metadata,
                                  time_unit=None,
                                  index_strategy="zorder",
                                  attribute_key_space=None,
                                  attribute_table=None):

        if attribute_key_space is not None or attribute_table is not None:
            self._construct_catalog(attribute_key_space, attribute_table)

        self._write_spacetime_multiband(layer_name,
                                        layer_zoom,
                                        rdd,
                                        metadata,
                                        time_unit,
                                        index_strategy)


class HBaseCatalog(_Catalog):

    __slots__ = ["pysc",
                 "avrogregistry",
                 "store",
                 "reader",
                 "writer",
                 "zookeepers",
                 "master",
                 "client_port",
                 "attribute_table"]

    def __init__(self,
                 pysc,
                 zookeepers,
                 master,
                 client_port,
                 avroregistry=None):

        super().__init__(pysc, avroregistry)
        self.pysc = pysc
        self._sc = self.pysc._jsc.sc()

        self.zookeepers = zookeepers
        self.master = master
        self.client_port = client_port

        self.avroregistry = avroregistry

        self.attribute_table = None

    def _construct_catalog(self, new_attribute_table):
        is_old = new_attribute_table != self.attribute_table
        is_none = new_attribute_table is not None

        if is_old and is_none:
            self.attribute_table = new_attribute_table
            self.store = \
                    self.store_factory.buildHBase(self.zookeepers,
                                                  self.master,
                                                  self.client_port,
                                                  self.attribute_table)
            self._construct_reader()
            self._construct_writer()

    def _construct_reader(self):
        self.reader = \
                self.reader_factory.buildHBase(self.store, self._sc)

    def _construct_writer(self):
        self.writer = \
                self.writer_factory.buildHBase(self.store,
                                               self.attribute_table)

    def query_spatial_singleband(self,
                                 attribute_table,
                                 layer_name,
                                 layer_zoom,
                                 intersects=None):

        self._construct_catalog(attribute_table)

        return self._query_spatial_singleband(layer_name,
                                              layer_zoom,
                                              intersects)

    def query_spatial_multiband(self,
                                attribute_table,
                                layer_name,
                                layer_zoom,
                                intersects=None):

        self._construct_catalog(attribute_table)

        return self._query_spatial_multiband(layer_name,
                                             layer_zoom,
                                             intersects)

    def query_spacetime_singleband(self,
                                   attribute_table,
                                   layer_name,
                                   layer_zoom,
                                   intersects=None,
                                   time_intervals=[]):

        self._construct_catalog(attribute_table)

        return self._query_spacetime_singleband(layer_name,
                                                layer_zoom,
                                                intersects,
                                                time_intervals)

    def query_spacetime_multiband(self,
                                  attribute_table,
                                  layer_name,
                                  layer_zoom,
                                  intersects=None,
                                  time_intervals=[]):

        self._construct_catalog(attribute_table)

        return self._query_spacetime_multiband(layer_name,
                                               layer_zoom,
                                               intersects,
                                               time_intervals)

    def write_spatial_singleband(self,
                                 layer_name,
                                 layer_zoom,
                                 rdd, metadata,
                                 index_strategy="zorder",
                                 attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spatial_singleband(layer_name,
                                       layer_zoom,
                                       rdd,
                                       metadata,
                                       index_strategy)

    def write_spatial_multiband(self,
                                layer_name,
                                layer_zoom,
                                rdd,
                                metadata,
                                index_strategy="zorder",
                                attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spatial_singleband(layer_name,
                                       layer_zoom,
                                       rdd,
                                       metadata,
                                       index_strategy)

    def write_spacetime_singleband(self,
                                   layer_name,
                                   layer_zoom,
                                   rdd, metadata,
                                   time_unit=None,
                                   index_strategy="zorder",
                                   attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spacetime_singleband(layer_name,
                                         layer_zoom,
                                         rdd,
                                         metadata,
                                         time_unit,
                                         index_strategy)

    def write_spacetime_multiband(self,
                                  layer_name,
                                  layer_zoom,
                                  rdd,
                                  metadata,
                                  time_unit=None,
                                  index_strategy="zorder",
                                  attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spacetime_multiband(layer_name,
                                        layer_zoom,
                                        rdd,
                                        metadata,
                                        time_unit,
                                        index_strategy)


class AccumuloCatalog(_Catalog):

    __slots__ = ["pysc",
                 "avrogregistry",
                 "store",
                 "reader",
                 "writer",
                 "zookeepers",
                 "instance_name",
                 "user",
                 "password",
                 "attribute_table"]

    def __init__(self,
                 pysc,
                 zookeepers,
                 instance_name,
                 user,
                 password,
                 avroregistry=None):

        super().__init__(pysc, avroregistry)
        self.pysc = pysc
        self._sc = self.pysc._jsc.sc()

        self.zookeepers = zookeepers
        self.instance_name = instance_name
        self.user = user
        self.password = password

        self.avroregistry = avroregistry

        self.attribute_table = None

    def _construct_catalog(self, new_attribute_table):
        is_old = new_attribute_table != self.attribute_table
        is_none = new_attribute_table is not None

        if is_old and is_none:
            self.attribute_table = new_attribute_table
            self.store = \
                    self.store_factory.buildAccumulo(self.zookeepers,
                                                     self.instance_name,
                                                     self.user,
                                                     self.password,
                                                     self.attribute_table)
            self._construct_reader()
            self._construct_writer()

    def _construct_reader(self):
        self.reader = \
                self.reader_factory.buildAccumulo(self.instance_name,
                                                  self.store,
                                                  self._sc)

    def _construct_writer(self):
        self.writer = \
                self.writer_factory.buildAccumulo(self.instance_name,
                                                  self.store,
                                                  self.attribute_table)

    def query_spatial_singleband(self,
                                 attribute_table,
                                 layer_name,
                                 layer_zoom,
                                 intersects=None):

        self._construct_catalog(attribute_table)

        return self._query_spatial_singleband(layer_name,
                                              layer_zoom,
                                              intersects)

    def query_spatial_multiband(self,
                                attribute_table,
                                layer_name,
                                layer_zoom,
                                intersects=None):

        self._construct_catalog(attribute_table)

        return self._query_spatial_multiband(layer_name,
                                             layer_zoom,
                                             intersects)

    def query_spacetime_singleband(self,
                                   attribute_table,
                                   layer_name,
                                   layer_zoom,
                                   intersects=None,
                                   time_intervals=[]):

        self._construct_catalog(attribute_table)

        return self._query_spacetime_singleband(layer_name,
                                                layer_zoom,
                                                intersects,
                                                time_intervals)

    def query_spacetime_multiband(self,
                                  attribute_table,
                                  layer_name,
                                  layer_zoom,
                                  intersects=None,
                                  time_intervals=[]):

        self._construct_catalog(attribute_table)

        return self._query_spacetime_multiband(layer_name,
                                               layer_zoom,
                                               intersects,
                                               time_intervals)

    def write_spatial_singleband(self,
                                 layer_name,
                                 layer_zoom,
                                 rdd,
                                 metadata,
                                 index_strategy="zorder",
                                 attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spatial_singleband(layer_name,
                                       layer_zoom,
                                       rdd,
                                       metadata,
                                       index_strategy)

    def write_spatial_multiband(self,
                                layer_name,
                                layer_zoom,
                                rdd,
                                metadata,
                                index_strategy="zorder",
                                attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spatial_multiband(layer_name,
                                      layer_zoom,
                                      rdd,
                                      metadata,
                                      index_strategy)

    def write_spacetime_singleband(self,
                                   layer_name,
                                   layer_zoom,
                                   rdd,
                                   metadata,
                                   time_unit=None,
                                   index_strategy="zorder",
                                   attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spacetime_singleband(layer_name,
                                         layer_zoom,
                                         rdd,
                                         metadata,
                                         time_unit,
                                         index_strategy)

    def write_spacetime_multiband(self,
                                  layer_name,
                                  layer_zoom,
                                  rdd,
                                  metadata,
                                  time_unit=None,
                                  index_strategy="zorder",
                                  attribute_table=None):

        if attribute_table is not None:
            self._construct_catalog(attribute_table)

        self._write_spacetime_multiband(layer_name,
                                        layer_zoom,
                                        rdd,
                                        metadata,
                                        time_unit,
                                        index_strategy)
