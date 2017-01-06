from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.keys import SpatialKey, SpaceTimeKey
from geopyspark.avroserializer import AvroSerializer

import io
import struct
import sys

def get_rdd(pysc):
    sc = pysc._jsc.sc()
    ew = pysc._gateway.jvm.SpaceTimeKeyWrapper

    tup = ew.testOut(sc)
    (java_rdd, schema) = (tup._1(), tup._2())

    ser = AvroSerializer(schema)
    return (RDD(java_rdd, pysc, AutoBatchedSerializer(ser)), schema)

def set_rdd(pysc, rdd, schema):
    ser = AvroSerializer(schema)
    dumped = rdd.map(lambda s: ser.dumps(s, schema))
    arrs = dumped.map(lambda s: bytearray(s))

    new_java_rdd = dumped._to_java_object_rdd()
    ew = pysc._gateway.jvm.SpaceTimeKeyWrapper

    ew.testIn(new_java_rdd.rdd(), schema)

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="extent-test")

    #java_import(sc._gateway.jvm, "geopyspark.geotrellis.SpatialKeyWrapper")
    java_import(sc._gateway.jvm, "geopyspark.geotrellis.SpaceTimeKeyWrapper")

    (keys, schema) = get_rdd(sc)

    new_keys = keys.map(lambda s: SpaceTimeKey(s.col + 1, s.row + 1, s.instant))
    set_rdd(sc, new_keys, schema)
