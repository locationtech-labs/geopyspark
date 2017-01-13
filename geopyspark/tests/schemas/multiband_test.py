#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer


def main(sc):
    path = "geopyspark.geotrellis.tests.schemas.ArrayMultibandTileWrapper"
    java_import(sc._gateway.jvm, path)

    (multiband_arrays, schema) = get_rdd(sc)

    new_multiband_arrays = multiband_arrays.map(lambda s: [x + 3 for x in s])

    set_rdd(sc, new_multiband_arrays, schema)

def get_rdd(pysc):
    sc = pysc._jsc.sc()
    mw = pysc._gateway.jvm.ArrayMultibandTileWrapper

    tup = mw.testOut(sc)
    (java_rdd, schema) = (tup._1(), tup._2())

    ser = AvroSerializer(schema)
    return (RDD(java_rdd, pysc, AutoBatchedSerializer(ser)), schema)

def set_rdd(pysc, rdd, schema):
    ser = AvroSerializer(schema)
    dumped = rdd.map(lambda s: ser.dumps(s, schema))
    dumped.collect()

    arrs = dumped.map(lambda s: bytearray(s))

    new_java_rdd = dumped._to_java_object_rdd()
    mw = pysc._gateway.jvm.ArrayMultibandTileWrapper

    mw.testIn(new_java_rdd.rdd(), schema)

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="multibandtile-test")
    main(sc)

