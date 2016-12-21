from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from spatial_key import SpatialKey

import io
import zlib
import struct
import sys


if __name__ == "__main__":
    schema = """{"type":"record","name":"SpatialKey","namespace":"geotrellis.spark","fields":[{"name":"col","type":"int"},{"name":"row","type":"int"}]}"""
    ser = AvroSerializer(schema)
    sc = SparkContext(master="local", appName="python-test")

    java_import(sc._gateway.jvm, "geopyspark.geotrellis.Box")

    testRdd = sc._gateway.jvm.Box.testRdd(sc._jsc.sc())
    encodedRdd = sc._gateway.jvm.Box.encodeRdd(testRdd)
    javaRdd = encodedRdd.toJavaRDD()

    pythonRdd = RDD(javaRdd, sc, AutoBatchedSerializer(ser))

    thing = pythonRdd.collect()
    print "THIS IS THE ITEM HELD WITHIN THE PYTHON RDD:", thing

    rdd2 = pythonRdd.map(lambda s: SpatialKey(s.col+1, s.row+1))
    result = rdd2.collect()
    print "THIS IS THE RESULT OF THE PYTHON RDD BEING MAPPED OVER:", result
