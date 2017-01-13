#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer


TILETYPES = ['Bit', 'Short', 'UShort', 'Byte', 'UByte', 'Int', 'Float', 'Double']

def main(sc):

    for x in TILETYPES:
        path = "geopyspark.geotrellis.tests.schemas." + x + "ArrayTileWrapper"

        java_import(sc._gateway.jvm, path)

        (tile_array, schema) = get_rdd(sc, x)

        new_tile_array = tile_array.map(lambda s: s + 1)

        set_rdd(sc, new_tile_array, schema, x)


def get_rdd(pysc, tile_type):
    sc = pysc._jsc.sc()

    tw = get_wrapper(pysc, tile_type)

    tup = tw.testOut(sc)
    (java_rdd, schema) = (tup._1(), tup._2())

    ser = AvroSerializer(schema)
    return (RDD(java_rdd, pysc, AutoBatchedSerializer(ser)), schema)

def set_rdd(pysc, rdd, schema, tile_type):
    ser = AvroSerializer(schema)
    dumped = rdd.map(lambda s: ser.dumps(s, schema))
    dumped.collect()

    arrs = dumped.map(lambda s: bytearray(s))

    new_java_rdd = dumped._to_java_object_rdd()
    tw = get_wrapper(pysc, tile_type)

    tw.testIn(new_java_rdd.rdd(), schema)

def get_wrapper(pysc, tile_type):
    if tile_type is 'Bit':
        return pysc._gateway.jvm.BitArrayTileWrapper

    elif tile_type is 'Short':
        return pysc._gateway.jvm.ShortArrayTileWrapper

    elif tile_type is 'UShort':
        return pysc._gateway.jvm.UShortArrayTileWrapper

    elif tile_type is 'Byte':
        return pysc._gateway.jvm.ByteArrayTileWrapper

    elif tile_type is 'UByte':
        return pysc._gateway.jvm.UByteArrayTileWrapper

    elif tile_type is 'Int':
        return pysc._gateway.jvm.IntArrayTileWrapper

    elif tile_type is 'Float':
        return pysc._gateway.jvm.FloatArrayTileWrapper

    else:
        return pysc._gateway.jvm.DoubleArrayTileWrapper

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="tile-test")
    main(sc)
