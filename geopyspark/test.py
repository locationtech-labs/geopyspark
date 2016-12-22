from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.spatial_key import SpatialKey
from geopyspark.avroserializer import AvroSerializer

import io
import zlib
import struct
import sys


if __name__ == "__main__":
    sc = SparkContext(master="local", appName="python-test")

    java_import(sc._gateway.jvm, "geopyspark.geotrellis.Box")
    java_import(sc._gateway.jvm, "geopyspark.geotrellis.ExtentWrapper")

    box_test_rdd = sc._gateway.jvm.Box.testRdd(sc._jsc.sc())
    box_encoded_rdd = sc._gateway.jvm.Box.encodeRdd(box_test_rdd)
    box_java_rdd = box_encoded_rdd.toJavaRDD()

    box_schema = sc._gateway.jvm.Box.keySchema()
    box_ser = AvroSerializer(box_schema)

    box_python_rdd = RDD(box_java_rdd, sc, AutoBatchedSerializer(box_ser))

    thing = box_python_rdd.collect()
    print "THIS IS THE ITEM HELD WITHIN THE PYTHON RDD:", thing

    rdd2 = box_python_rdd.map(lambda s: SpatialKey(s.col+1, s.row+1))
    result = rdd2.collect()
    print "THIS IS THE RESULT OF THE PYTHON RDD BEING MAPPED OVER:", result
