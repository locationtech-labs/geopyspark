#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.tile import TileArray
from geopyspark.avroregistry import AvroRegistry

import numpy as np
import unittest

class MultibandSchemaTest(unittest.TestCase):
    pysc = SparkContext(master="local", appName="multibandtile-test")
    path = "geopyspark.geotrellis.tests.schemas.ArrayMultibandTileWrapper"
    java_import(pysc._gateway.jvm, path)

    arr = TileArray(np.array(bytearray([0, 0, 1, 1])).reshape(2, 2), -128)
    multiband_tile = [arr, arr, arr]

    def get_rdd(self):
        sc = self.pysc._jsc.sc()
        mw = self.pysc._gateway.jvm.ArrayMultibandTileWrapper

        tup = mw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_multibands(self):
        (multibands, schema) = self.get_rdd()

        return multibands.collect()

    def test_encoded_multibands(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: AvroRegistry.multiband_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
                {'bands': [AvroRegistry.tile_encoder(x) for x in self.multiband_tile]},
                {'bands': [AvroRegistry.tile_encoder(x) for x in self.multiband_tile]},
                {'bands': [AvroRegistry.tile_encoder(x) for x in self.multiband_tile]},
                ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_multibands(self):
        actual_multibands = self.get_multibands()

        expected_multibands = [
                self.multiband_tile,
                self.multiband_tile,
                self.multiband_tile
                ]

        for actual_tiles, expected_tiles in zip(actual_multibands, expected_multibands):
            for actual, expected in zip(actual_tiles, expected_tiles):
                self.assertTrue((actual == expected).all())


if __name__ == "__main__":
    unittest.main()
