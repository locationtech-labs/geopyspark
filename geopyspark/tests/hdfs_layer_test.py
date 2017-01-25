#!/bin/env python3

import sys
import calendar
import time

from pyspark import SparkContext
from shapely.geometry import Polygon
from geopyspark.geotrellis.catalog import HadoopCatalog


if __name__ == "__main__":
    if len(sys.argv) > 3:
        uri = sys.argv[1]
        layer_name = sys.argv[2]
        layer_zoom = int(sys.argv[3])
    else:
        exit(-1)

    polygon = Polygon([(-87.1875, 34.43409789359469), (-78.15673828125, 34.43409789359469), (-78.15673828125, 39.87601941962116), (-87.1875, 39.87601941962116)])

    sc = SparkContext(appName="hdfs-layer-test")
    catalog = HadoopCatalog(uri, sc)
    (rdd, metadata) = catalog.query("spatial", "singleband", layer_name, layer_zoom, polygon)
    new_layer_name = layer_name + "-" + str(calendar.timegm(time.gmtime()))
    catalog.write(new_layer_name, layer_zoom, rdd, metadata, index_strategy="zorder")
