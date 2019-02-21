import geopyspark as gps

from pyspark import SparkContext
from shapely.geometry import box

import sys
from random import random
from operator import add


# Create the SparkContext
conf = gps.geopyspark_conf(appName="geopyspark-example", master="local[*]")
sc = SparkContext(conf=conf)

partitions = 1

n = 100000 * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

count = sc.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
print("Pi is roughly %f" % (4.0 * count / n))
