#!/bin/env python3

import sys
import calendar
import time

from pyspark import SparkContext
from shapely.geometry import Polygon
from geopyspark.geotrellis.catalog import S3Catalog


if __name__ == "__main__":
    if len(sys.argv) > 4:
        bucket = sys.argv[1]
        root = sys.argv[2]
        layer_name = sys.argv[3]
        layer_zoom = int(sys.argv[4])
    else:
        exit(-1)

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
    polygon = Polygon([(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)])

    sc = SparkContext(appName="s3-layer-test")
    catalog = S3Catalog(bucket, root, sc)
    (rdd, metadata) = catalog.query("spatial", "singleband", layer_name, layer_zoom, polygon)
    print(rdd.take(1))
    print(rdd.count())
