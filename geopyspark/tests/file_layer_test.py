#!/bin/env python3

import sys
import calendar
import time

from pyspark import SparkContext
from shapely.geometry import Polygon
from geopyspark.geotrellis.catalog import FileCatalog


if __name__ == "__main__":
    if len(sys.argv) > 3:
        path = sys.argv[1]
        layer_name = sys.argv[2]
        layer_zoom = int(sys.argv[3])
    else:
        exit(-1)

    sc = SparkContext(appName="file-layer-test")
    catalog = FileCatalog(path, sc)
    (rdd, metadata) = catalog.query("spatial", "singleband", layer_name, layer_zoom)
    new_layer_name = layer_name + "-" + str(calendar.timegm(time.gmtime()))
    catalog.write(new_layer_name, layer_zoom, rdd, metadata)
