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
    if len(sys.argv) > 4:
        bucket = sys.argv[1]
        root = sys.argv[2]
        layer_name = sys.argv[3]
        layer_zoom = int(sys.argv[4])
    else:
        exit(-1)

    sc = SparkContext(appName="s3-layer-test")

    store_factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
    store = store_factory.buildS3(bucket, root)
    metadata = store.metadataSpatial(layer_name, layer_zoom)
    print(metadata.get())

    extent_xmin = -20037508.342789244
    extent_ymin = -20037508.342789244
    extent_xmax = 20037508.342789244
    extent_ymax = 20037508.342789244
    xdelta = (extent_xmax - extent_xmin) / 1
    ydelta = (extent_ymax - extent_ymin) / 1
    xmin = extent_xmin
    xmax = extent_xmin + xdelta
    ymin = extent_ymin
    ymax = extent_ymin + ydelta

    reader_factory = sc._gateway.jvm.geopyspark.geotrellis.io.LayerReaderFactory
    reader = reader_factory.buildS3(store, sc._jsc.sc())
    polygon = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])
    tup = reader.query("spatial", "singleband", layer_name, layer_zoom, dumps(polygon), [])
    (jrdd, schema) = (tup._1(), tup._2())
    serializer = AvroSerializer(schema)
    rdd = RDD(jrdd, sc, AutoBatchedSerializer(serializer))
    print(rdd.take(1))
    print(rdd.count())
