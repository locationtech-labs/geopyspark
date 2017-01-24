#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer

import io
import sys


def get_rdd(pysc):
    sc = pysc._jsc.sc()
    pew = pysc._gateway.jvm.ProjectedExtentWrapper

    tup = pew.testOut(sc)
    (java_rdd, schema) = (tup._1(), tup._2())

    ser = AvroSerializer(schema)
    return (RDD(java_rdd, pysc, AutoBatchedSerializer(ser)), schema)

def set_rdd(pysc, rdd, schema):
    ser = AvroSerializer(schema)
    dumped = rdd.map(lambda s: ser.dumps(s, schema))
    arrs = dumped.map(lambda s: bytearray(s))

    new_java_rdd = dumped._to_java_object_rdd()
    ew = pysc._gateway.jvm.ProjectedExtentWrapper

    ew.testIn(new_java_rdd.rdd(), schema)

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="projectedextent-test")

    java_import(sc._gateway.jvm, "geopyspark.geotrellis.ProjectedExtentWrapper")

    (extents, schema) = get_rdd(sc)
    set_rdd(sc, extents, schema)
