#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.extent import Extent
from geopyspark.avroserializer import AvroSerializer

import io
import sys


def get_rdd(pysc):
    sc = pysc._jsc.sc()
    ew = pysc._gateway.jvm.ExtentWrapper

    tup = ew.testOut(sc)
    (java_rdd, schema) = (tup._1(), tup._2())
    print(type(java_rdd))
    print(schema)

    ser = AvroSerializer(schema)
    return (RDD(java_rdd, pysc, AutoBatchedSerializer(ser)), schema)

def set_rdd(pysc, rdd, schema):
    ew = pysc._gateway.jvm.ExtentWrapper

    ew.testIn(rdd, schema)

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="extent-test")

    java_import(sc._gateway.jvm, "geopyspark.geotrellis.ExtentWrapper")

    (extents, schema) = get_rdd(sc)

    new_extents = extents.map(lambda s: Extent(s.xmin+1, s.ymin+1, s.xmax+1, s.ymax+1))
    print(new_extents.count())

    set_rdd(sc, new_extents, schema)
