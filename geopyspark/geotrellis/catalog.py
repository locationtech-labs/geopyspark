from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry

from shapely.geometry import Point, Polygon
from shapely.wkt import dumps, loads

import time
import calendar


class Metadata:
    def __init__(self, key_type, value_type, schema, jmetadata):
        self.key_type = key_type
        self.value_type = value_type
        self.schema = schema
        self.jmetadata = jmetadata


class Catalog:

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
        return self._store.metadataSpatial(layer_name, layer_zoom)

    def _get_spacetime_jmetadata(self, layer_name, layer_zoom):
        return self._store.metadataSpaceTime(layer_name, layer_zoom)

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
            tup = reader_method(layer_name, layer_zoom, dumps(intersects), time_intervals)
        elif isinstance(intersects, str):
            tup = reader_method(layer_name, layer_zoom, intersects, time_intervals)
        else:
            raise BaseException("Bad polygon")

        schema = tup._2()

        jmetadata = self._get_spacetime_jmetadata(layer_name, layer_zoom)
        metadata = Metadata("spacetime", value_type, schema, jmetadata)

        return self._construct_return(tup._1(), self.schema, metadata)

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

        writer.writeSpaceTimeSingleband(layer_name,
                layer_zoom,
                rdd._jrdd,
                jmetadata,
                schema,
                time_unit,
                index_strategy)


class HadoopCatalog(Catalog):

    def __init__(self, uri, sc):
        self.sc = sc
        store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        self.store = store_factory.buildHadoop(uri, sc._jsc.sc())
        reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
        self.reader = reader_factory.buildHadoop(self.store)
        writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
        self.writer = writer_factory.buildHadoop(self.store)


class S3Catalog(Catalog):

    def __init__(self, bucket, root, sc):
        self.sc = sc
        store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        self.store = store_factory.buildS3(bucket, root)
        reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
        self.reader = reader_factory.buildS3(self.store, sc._jsc.sc())
        writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
        self.writer = writer_factory.buildS3(self.store)


class FileCatalog(Catalog):

    def __init__(self, pysc, avroregistry=None):
        self.pysc = pysc
        self._sc = pysc._jsc.sc()

        self.avroregistry = avroregistry

        self.path = None
        self._store = None
        self._reader = None
        self._writer = None

    '''
    def _construct_catalog(self, new_path):
        if new_path != self.path and new_path is not None:
            self.path = new_path
            self._store = self.store_factory.buildFile(self.path)
            self._construct_reader()
            self._construct_writer()
    '''

    def _construct_reader(self):
        self._reader = self.reader_factory.buildFile(self._store, self._sc)

    def _construct_writer(self):
        self._writer = self.writer_factory.buildFile(self._store)

    def query_spatial_singleband(self,
            path,
            layer_name,
            layer_zoom,
            intersects=None):
        self._construct_catalog(path)
        self.path = path

        if intersects is None:
            reader_method = self._reader.readSpatialSingleband
            return self._read_fully(
                    "spatial",
                    "singleband",
                    reader_method,
                    layer_name,
                    layer_zoom)

        else:
            reader_method = self._reader.querySpatialSingleband
            return self._query_spatial(
                    reader_method,
                    layer_name,
                    layer_zoom,
                    intersects)

    def query_spatial_multiband(self,
            path,
            layer_name,
            layer_zoom,
            intersects=None):
        self._construct_catalog(path)

        if intersects is None:
            reader_method = self._reader.readSpatialMultiband
            return self._read_fully(
                    "spatial",
                    "multiband",
                    reader_method,
                    layer_name,
                    layer_zoom)

        else:
            reader_method = self._reader.querySpatialMultiband
            return self._query_spatial(
                    reader_method,
                    layer_name,
                    layer_zoom,
                    intersects)

    def query_spacetime_singleband(self,
            path,
            layer_name,
            layer_zoom,
            intersects=None,
            time_intervals=[]):
        self._construct_catalog(path)

        if intersects is None:
            reader_method = self._reader.readSpaceTimeSingleband
            return self._read_fully(
                    "spacetime",
                    "singleband",
                    reader_method,
                    layer_name,
                    layer_zoom,
                    time_intervals)

        else:
            reader_method = self._reader.querySpaceTimeSingleband
            return self._query_spacetime(
                    reader_method,
                    layer_name,
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

        if intersects is None:
            reader_method = self._reader.readSpaceTimeMultiband
            return self._read_fully(
                    "spacetime",
                    "multiband",
                    reader_method,
                    layer_name,
                    layer_zoom,
                    time_intervals)

        else:
            reader_method = self._reader.querySpaceTimeMultiband
            return self._query_spacetime(
                    reader_method,
                    layer_name,
                    layer_zoom,
                    intersects,
                    time_intervals)

    def write_spatial_singleband(
            self,
            layer_name,
            layer_zoom,
            rdd,
            metadata,
            path=None,
            index_strategy="zorder"):

        if path is not None:
            self._construct_catalog(path)
        self.path = path

        writer_method = self._writer.writeSpatialSingleband
        self._write_spatial(
                writer_method,
                layer_name,
                layer_zoom,
                rdd,
                metadata,
                index_strategy)

    def write_spatial_multiband(self,
            layer_name,
            layer_zoom,
            rdd,
            metadata,
            path=None,
            index_strategy="zorder"):

        if path is not None:
            self._construct_catalog(path)

        writer_method = self._writer.writeSpatialMultiband
        self._write_spatial(
                writer_method,
                layer_name,
                layer_zoom,
                rdd,
                metadata,
                index_strategy)

    def write_spacetime_singleband(
            self,
            layer_name,
            layer_zoom,
            rdd,
            metadata,
            path=None,
            index_strategy="zorder"):

        if path is not None:
            self._construct_catalog(path)

        writer_method = self._writer.writeSpaceTimeSingleband
        self._write_spacetime(
                writer_method,
                layer_name,
                layer_zoom,
                rdd,
                metadata,
                index_strategy)

    def write_spacetime_multiband(
            self,
            layer_name,
            layer_zoom,
            rdd,
            metadata,
            path=None,
            index_strategy="zorder"):

        if path is not None:
            self._construct_catalog(path)

        writer_method = self._writer.writeSpaceTimeMultiband
        self._write_spacetime(
                writer_method,
                layer_name,
                layer_zoom,
                rdd,
                metadata,
                index_strategy)


class CassandraCatalog(Catalog):

    def __init__(self, hosts, username, password, replicationStrategy, replicationFactor, localDc, usedHostsPerRemoteDc, allowRemoteDCsForLocalConsistencyLevel, attributeKeySpace, attributeTable, sc):
        self.sc = sc
        store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        self.store = store_factory.buildCassandra(hosts, username, password, replicationStrategy, replicationFactor, localDc, usedHostsPerRemoteDc, allowRemoteDCsForLocalConsistencyLevel, attributeKeySpace, attributeTable)
        reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
        self.reader = reader_factory.buildCassandra(self.store, sc._jsc.sc())
        writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
        self.writer = writer_factory.buildCassandra(self.store, attributeKeySpace, attributeTable)


class HBaseCatalog(Catalog):

    def __init__(self, zookeepers, master, clientPort, attributeTable, sc):
        self.sc = sc
        store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        self.store = store_factory.buildHBase(zookeepers, master, clientPort, attributeTable)
        reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
        self.reader = reader_factory.buildHBase(self.store, sc._jsc.sc())
        writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
        self.writer = writer_factory.buildHBase(self.store, attributeTable)


class AccumuloCatalog(Catalog):

    def __init__(self, zookeepers, instanceName, user, password, attributeTable, sc):
        self.sc = sc
        store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        self.store = store_factory.buildAccumulo(zookeepers, instanceName, user, password, attributeTable)
        reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
        self.reader = reader_factory.buildAccumulo(instanceName, self.store, sc._jsc.sc())
        writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
        self.writer = writer_factory.buildAccumulo(instanceName, self.store, attributeTable)
