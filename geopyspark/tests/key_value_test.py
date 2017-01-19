#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.tile import TileArray

import io
import sys
import numpy as np


def get_rdd(pysc):
    sc = pysc._jsc.sc()
    ew = pysc._gateway.jvm.KeyValueRecordWrapper

    tup = ew.testOut(sc)
    (java_rdd, schema) = (tup._1(), tup._2())

    ser = AvroSerializer(schema)
    return (RDD(java_rdd, pysc, AutoBatchedSerializer(ser)), schema)

def set_rdd(pysc, rdd, schema):
    ser = AvroSerializer(schema)
    dumped = rdd.map(lambda s: ser.dumps(s, schema))
    dumped.collect()

    arrs = dumped.map(lambda s: bytearray(s))

    new_java_rdd = dumped._to_java_object_rdd()
    ew = pysc._gateway.jvm.KeyValueRecordWrapper

    ew.testIn(new_java_rdd.rdd(), schema)

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="key-value-test")

    java_import(sc._gateway.jvm, "geopyspark.geotrellis.KeyValueRecordWrapper")

    (tuples, schema) = get_rdd(sc)

    new_tuples = tuples.map(lambda s: [(x[0] + 1, x[1]) for x in s])
    set_rdd(sc, new_tuples, schema)
