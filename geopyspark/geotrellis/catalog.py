from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.avroserializer import AvroSerializer

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

    def query(self, key_type, value_type, layer_name, layer_zoom, intersects=None):
        # check parameters
        if not key_type in ["spatial", "spacetime"]:
            raise BaseException("Bad key_type")
        if not value_type in ["singleband", "multiband"]:
            raise BaseException("Bad value_type")
        if not isinstance(layer_name, str):
            raise BaseException("Bad layer_name")
        if not isinstance(layer_zoom, int):
            raise BaseException("Bad layer_zoom")

        # perform query
        if isinstance(intersects, Polygon):
            tup = self.reader.query(key_type, value_type, layer_name, layer_zoom, dumps(intersects), [])
        elif isinstance(intersects, str):
            tup = self.reader.query(key_type, value_type, layer_name, layer_zoom, intersects, [])
        elif intersects == None:
            tup = self.reader.read(key_type, value_type, layer_name, layer_zoom)
        else:
            raise BaseException("Bad polygon")

        # construct metadata
        schema = tup._2()
        if key_type == "spatial":
            jmetadata = self.store.metadataSpatial(layer_name, layer_zoom)
        else:
            jmetadata = self.store.metadataSpacetime(layer_name, layer_zoom)
        metadata = Metadata(key_type, value_type, schema, jmetadata)

        # construct rdd
        jrdd = tup._1()
        serializer = AvroSerializer(schema)
        rdd = RDD(jrdd, self.sc, AutoBatchedSerializer(serializer))

        # return rdd and metadata
        return (rdd, metadata)

    def write(self, layer_name, layer_zoom, rdd, metadata, time_unit=None, index_strategy="zorder"):
        # check parameters
        if not isinstance(layer_name, str):
            raise BaseException("Bad layer_name")
        if not isinstance(layer_zoom, int):
            raise BaseException("Bad layer_zoom")
        if not isinstance(rdd, RDD):
            raise BaseException("Bad rdd")
        if not isinstance(metadata, Metadata):
            raise BaseException("Bad metadata")
        if not (isinstance(time_unit, str) or time_unit == None):
            raise BaseException("Bad time_unit")
        if not index_strategy in ["zorder", "hilbert", "rowmajor"]:
            raise BaseException("Bad index_strategy")

        # gather parameters for call to Scala method
        key_type = metadata.key_type
        value_type = metadata.value_type
        schema = metadata.schema
        jmetadata = metadata.jmetadata
        if time_unit == None:
            time_unit = ""

        # write the data
        self.writer.write(key_type, value_type, layer_name, layer_zoom, rdd._jrdd, jmetadata, schema, time_unit, index_strategy)


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

    def __init__(self, path, sc):
        self.sc = sc
        store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
        self.store = store_factory.buildFile(path)
        reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
        self.reader = reader_factory.buildFile(self.store, sc._jsc.sc())
        writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
        self.writer = writer_factory.buildFile(self.store)


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
