from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.geotrellis.extent import Extent
from geopyspark.geotrellis.tile import TileArray
from geopyspark.avroregistry import AvroRegistry

import numpy as np
import unittest
import pytest


class TupleSchemaTest(unittest.TestCase):

    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="tuple-test")
        self.path = "geopyspark.geotrellis.tests.schemas.TupleWrapper"
        java_import(self.pysc._gateway.jvm, self.path)

        self.extents = [Extent(0, 0, 1, 1), Extent(1, 2, 3, 4), Extent(5, 6, 7, 8)]
        self.arrs = [
                TileArray(np.array([0, 1, 2, 3, 4, 5]).reshape(3, 2), -2147483648),
                TileArray(np.array([0, 1, 2, 3, 4, 5]).reshape(2, 3), -2147483648),
                TileArray(np.array([0, 1, 2, 3, 4, 5]).reshape(6, 1), -2147483648)
                ]

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.pysc.stop()
        self.pysc._gateway.close()

    def get_rdd(self):
        sc = self.pysc._jsc.sc()
        ew = self.pysc._gateway.jvm.TupleWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tuples(self):
        (tuples, schema) = self.get_rdd()

        return tuples.collect()

    def test_encoded_tuples(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: AvroRegistry.tuple_encoder(s,
            AvroRegistry.tile_encoder,
            AvroRegistry.extent_encoder))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'_1': AvroRegistry.tile_encoder(self.arrs[0]),
                    '_2': AvroRegistry.extent_encoder(self.extents[0])},
                {'_1': AvroRegistry.tile_encoder(self.arrs[1]),
                    '_2': AvroRegistry.extent_encoder(self.extents[1])},
                {'_1': AvroRegistry.tile_encoder(self.arrs[2]),
                    '_2': AvroRegistry.extent_encoder(self.extents[2])}
                ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual['_1'], expected['_1'])

    def test_decoded_tuples(self):
        actual_tuples = self.get_tuples()

        expected_tuples = [
                (self.arrs[0], self.extents[0]),
                (self.arrs[1], self.extents[1]),
                (self.arrs[2], self.extents[2])
                ]

        for actual, expected in zip(actual_tuples, expected_tuples):
            (actual_tile, actual_extent) = actual
            (expected_tile, expected_extent) = expected

            self.assertTrue((actual_tile == expected_tile).all())
            self.assertEqual(actual_extent, expected_extent)


if __name__ == "__main__":
    unittest.main()
