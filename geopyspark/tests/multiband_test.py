from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.geotrellis.tile import TileArray
from geopyspark.avroregistry import AvroRegistry
from geopyspark.tests.base_test_class import BaseTestClass

import numpy as np
import unittest
import pytest


class MultibandSchemaTest(BaseTestClass):
    path = "geopyspark.geotrellis.tests.schemas.ArrayMultibandTileWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    arr = TileArray(np.array(bytearray([0, 0, 1, 1])).reshape(2, 2), -128)
    multiband_tile = [arr, arr, arr]

    def get_rdd(self):
        sc = BaseTestClass.pysc._jsc.sc()
        mw = BaseTestClass.pysc._gateway.jvm.ArrayMultibandTileWrapper

        tup = mw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

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
