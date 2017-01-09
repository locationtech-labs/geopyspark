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

    sc = SparkContext(appName="layer-test")

    store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
    store = store_factory.build("hdfs", uri, sc._jsc.sc())
    header = store.header(layer_name, layer_zoom)
    cell_type = store.cellType(layer_name, layer_zoom)

    reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
    reader = reader_factory.build("hdfs", uri, sc._jsc.sc())
    polygon = Polygon([(-87.1875, 34.43409789359469), (-78.15673828125, 34.43409789359469), (-78.15673828125, 39.87601941962116), (-87.1875, 39.87601941962116)])
    tup = reader.query(layer_name, layer_zoom, dumps(polygon))
    (jrdd, schema) = (tup._1(), tup._2())
    serializer = AvroSerializer(schema)
    rdd = RDD(jrdd, sc, AutoBatchedSerializer(serializer))
    print(rdd.take(1))

    writer_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerWriterFactory
    writer = writer_factory.build("hdfs", uri, sc._jsc.sc())
    writer.write(layer_name + "-" + str(calendar.timegm(time.gmtime())), 0, rdd._jrdd, schema, layer_name, layer_zoom)
