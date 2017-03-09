from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry
from geopyspark.tests.base_test_class import BaseTestClass

import numpy as np
import unittest


def decode(x):
    return (AvroRegistry.tile_decoder(x['_1']), x['_2'])

def encode(x):
    return {'_1': AvroRegistry.tile_encoder(x[0]), '_2': x[1]}


class TupleSchemaTest(BaseTestClass):
    path = "geopyspark.geotrellis.tests.schemas.TupleWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    extents = [
        {'xmin': 0, 'ymin': 0, 'xmax': 1, 'ymax': 1},
        {'xmin': 1, 'ymin': 2, 'xmax': 3, 'ymax': 4},
        {'xmin': 5, 'ymin': 6, 'xmax': 7, 'ymax': 8}
    ]

    arrs = [
        {'data': np.array([0, 1, 2, 3, 4, 5]).reshape(3, 2), 'no_data_value': -2147483648},
        {'data': np.array([0, 1, 2, 3, 4, 5]).reshape(2, 3), 'no_data_value': -2147483648},
        {'data': np.array([0, 1, 2, 3, 4, 5]).reshape(6, 1), 'no_data_value': -2147483648}
    ]

    def get_rdd(self):
        sc = BaseTestClass.pysc._jsc.sc()
        ew = BaseTestClass.pysc._gateway.jvm.TupleWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema, decode, encode)
        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def test_encoded_tuples(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: encode(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'_1': AvroRegistry.tile_encoder(self.arrs[0]), '_2': self.extents[0]},
            {'_1': AvroRegistry.tile_encoder(self.arrs[1]), '_2': self.extents[1]},
            {'_1': AvroRegistry.tile_encoder(self.arrs[2]), '_2': self.extents[2]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertDictEqual(actual, expected)

    def test_decoded_tuples(self):
        (tuples, schema) = self.get_rdd()
        actual_tuples = tuples.collect()

        expected_tuples = [
            (self.arrs[0], self.extents[0]),
            (self.arrs[1], self.extents[1]),
            (self.arrs[2], self.extents[2])
        ]

        for actual, expected in zip(actual_tuples, expected_tuples):
            (actual_tile, actual_extent) = actual
            (expected_tile, expected_extent) = expected

            self.assertTrue((actual_tile['data'] == expected_tile['data']).all())
            self.assertDictEqual(actual_extent, expected_extent)


if __name__ == "__main__":
    unittest.main()
