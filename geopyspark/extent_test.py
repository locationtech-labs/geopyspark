#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.extent import Extent
from geopyspark.avroserializer import AvroSerializer

import io
import sys


if __name__ == "__main__":
    sc = SparkContext(master="local", appName="extent-test")

    java_import(sc._gateway.jvm, "geopyspark.geotrellis.ExtentWrapper")

    test_rdd = sc._gateway.jvm.ExtentWrapper.testRdd(sc._jsc.sc())
    encoded_rdd = sc._gateway.jvm.ExtentWrapper.encodeRdd(test_rdd)
    java_rdd = encoded_rdd.toJavaRDD()

    schema = sc._gateway.jvm.ExtentWrapper.keySchema()

    ser = AvroSerializer(schema)

    python_rdd = RDD(java_rdd, sc, AutoBatchedSerializer(ser))
    python_rdd2 = python_rdd.map(lambda s: Extent(s.xmin+1, s.ymin+1, s.xmax+1, s.ymax+1))
    python_rdd3 = python_rdd2.map(lambda s: ser.dumps(s, schema))
    final_python_rdd = python_rdd3.map(lambda s: bytearray(s))

    collection = python_rdd2.collect()

    print '\n\n\n'
    print "THESE ARE THE EXTENTS THAT HAVE BEEN MODIFIED ON THE PYTHON SIDE"
    for x in collection: print x
    print '\n\n\n'

    new_java_rdd = final_python_rdd._to_java_object_rdd()
    sc._gateway.jvm.ExtentWrapper.makeRasterExtent(new_java_rdd.rdd())
