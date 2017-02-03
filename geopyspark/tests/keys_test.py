from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.geotrellis.keys import SpatialKey, SpaceTimeKey
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry

import unittest
import pytest


class SpatialKeySchemaTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="spatial-key-test")
        self.path = "geopyspark.geotrellis.tests.schemas.SpatialKeyWrapper"
        java_import(self.pysc._gateway.jvm, self.path)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.pysc.stop()
        self.pysc._gateway.close()

    def get_rdd(self):
        sc = self.pysc._jsc.sc()
        ew = self.pysc._gateway.jvm.SpatialKeyWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_skeys(self):
        (skeys, schema) = self.get_rdd()

        return skeys.collect()

    def test_encoded_skeys(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.spatial_key_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'col': 7, 'row': 3}
                ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_extents(self):
        actual_skeys = self.get_skeys()

        expected_skeys = [
                SpatialKey(7, 3),
                ]

        for actual, expected in zip(actual_skeys, expected_skeys):
            self.assertEqual(actual, expected)


class SpaceTimeKeySchemaTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="spacetime-key-test")
        self.path = "geopyspark.geotrellis.tests.schemas.SpaceTimeKeyWrapper"
        java_import(self.pysc._gateway.jvm, self.path)

    def tearDown(self):
        self.pysc.stop()
        self.pysc._gateway.close()

    def get_rdd(self):
        java_import(self.pysc._gateway.jvm, self.path)
        sc = self.pysc._jsc.sc()
        ew = self.pysc._gateway.jvm.SpaceTimeKeyWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_skeys(self):
        (skeys, schema) = self.get_rdd()

        return skeys.collect()

    def test_encoded_skeys(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.spacetime_key_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'col': 7, 'row': 3, 'instant': 5},
                {'col': 9, 'row': 4, 'instant': 10},
                {'col': 11, 'row': 5, 'instant': 15}
                ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_extents(self):
        actual_skeys = self.get_skeys()

        expected_skeys = [
                SpaceTimeKey(7, 3, 5),
                SpaceTimeKey(9, 4, 10),
                SpaceTimeKey(11, 5, 15)
                ]

        for actual, expected in zip(actual_skeys, expected_skeys):
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
