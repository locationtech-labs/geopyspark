from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.geotrellis.extent import Extent
from geopyspark.geotrellis.tile import TileArray
from geopyspark.avroregistry import AvroRegistry
from geopyspark.tests.base_test_class import BaseTestClass

import numpy as np
import unittest
import pytest


@pytest.mark.xfail
class KeyValueRecordSchemaTest(unittest.TestCase):
    path = "geopyspark.geotrellis.tests.schemas.KeyValueRecordWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    extents = [Extent(0, 0, 1, 1), Extent(1, 2, 3, 4), Extent(5, 6, 7, 8)]
    arrs = [
        TileArray(np.array(bytearray([0, 1, 2, 3, 4, 5])).reshape(3, 2), -128),
        TileArray(np.array(bytearray([0, 1, 2, 3, 4, 5])).reshape(2, 3), -128),
        TileArray(np.array(bytearray([0, 1, 2, 3, 4, 5])).reshape(6, 1), -128)
    ]

    tuple_list= [
        (arrs[0], extents[0]),
        (arrs[1], extents[1]),
        (arrs[2], extents[2])
    ]

    def get_rdd(self):
        sc = BaseTestClass.pysc._jsc.sc()
        ew = BaseTestClass.pysc._gateway.jvm.KeyValueRecordWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def get_kvs(self):
        (kvs, schema) = self.get_rdd()

        return kvs.collect()

    def test_encoded_kvs(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: AvroRegistry().key_value_record_encoder(s))

        actual_encoded = encoded.collect()

        pairs = [AvroRegistry.tuple_encoder(x,
                                            AvroRegistry.tile_encoder,
                                            AvroRegistry.extent_encoder) for x in self.tuple_list]

        expected_encoded = [
            {'pairs': pairs},
            {'pairs': pairs},
        ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_kvs(self):
        actual_kvs = self.get_kvs()

        expected_kvs = [
            self.tuple_list,
            self.tuple_list
        ]

        for actual_tuples, expected_tuples in zip(actual_kvs, expected_kvs):
            for actual, expected in zip(actual_tuples, expected_tuples):
                (actual_tile, actual_extent) = actual
                (expected_tile, expected_extent) = expected

                self.assertTrue((actual_tile == expected_tile).all())
                self.assertEqual(actual_extent, expected_extent)


if __name__ == "__main__":
    unittest.main()
