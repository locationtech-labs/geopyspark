#!/bin/env python3

import sys

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.avroserializer import AvroSerializer

from shapely.geometry import Point, Polygon
from shapely.wkt import dumps, loads

import time
import calendar


if __name__ == "__main__":
    if len(sys.argv) > 3:
        uri = sys.argv[1]
        layer_name = sys.argv[2]
        layer_zoom = int(sys.argv[3])
    else:
        exit(-1)

    sc = SparkContext(appName="hdfs-layer-test")

    store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
    store = store_factory.buildHadoop(uri, sc._jsc.sc())
    metadata = store.metadataSpatial(layer_name, layer_zoom)
    print(metadata.get())

    reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
    reader = reader_factory.buildHadoop(store)
    polygon = Polygon([(-87.1875, 34.43409789359469), (-78.15673828125, 34.43409789359469), (-78.15673828125, 39.87601941962116), (-87.1875, 39.87601941962116)])
    tup = reader.query("spatial", "singleband", layer_name, layer_zoom, dumps(polygon), [])
    # tup = reader.read("spatial", "singleband", layer_name, layer_zoom)
    (jrdd, schema) = (tup._1(), tup._2())
    serializer = AvroSerializer(schema)
    rdd = RDD(jrdd, sc, AutoBatchedSerializer(serializer))
    print(rdd.take(1))

    writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
    writer = writer_factory.buildHadoop(store)
    new_layer_name = layer_name + "-" + str(calendar.timegm(time.gmtime()))
    writer.write("spatial", "singleband", "", new_layer_name, 0, rdd._jrdd, schema, metadata)
