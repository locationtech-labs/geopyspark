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


def decode(xs):
    tuples = xs['pairs']
    return [(AvroRegistry.tile_decoder(x['_1']), x['_2']) for x in tuples]

def encode(xs):
    return [{'_1': AvroRegistry.tile_encoder(x[0]), '_2': x[1]} for x in xs]


class KeyValueRecordSchemaTest(unittest.TestCase):
    path = "geopyspark.geotrellis.tests.schemas.KeyValueRecordWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    extents = [
        {'xmin': 0, 'ymin': 0, 'xmax': 1, 'ymax': 1},
        {'xmin': 1, 'ymin': 2, 'xmax': 3, 'ymax': 4},
        {'xmin': 5, 'ymin': 6, 'xmax': 7, 'ymax': 8}
    ]

    arrs = [
        {'data': np.array([0, 1, 2, 3, 4, 5]).reshape(3, 2), 'no_data_value': -128},
        {'data': np.array([0, 1, 2, 3, 4, 5]).reshape(2, 3), 'no_data_value': -128},
        {'data': np.array([0, 1, 2, 3, 4, 5]).reshape(6, 1), 'no_data_value': -128}
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

        ser = AvroSerializer(schema, decode, encode)
        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def test_encoded_kvs(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: encode(s))

        actual_encoded = encoded.collect()

        encoded_tuples = [
            {'_1': AvroRegistry.tile_encoder(self.arrs[0]), '_2': self.extents[0]},
            {'_1': AvroRegistry.tile_encoder(self.arrs[1]), '_2': self.extents[1]},
            {'_1': AvroRegistry.tile_encoder(self.arrs[2]), '_2': self.extents[2]}
        ]

        expected_encoded = [
            {'pairs': encoded_tuples},
            {'pairs': encoded_tuples},
        ]

        print(actual_encoded)
        print('\n\n')
        print(expected_encoded)

    def test_decoded_kvs(self):
        (kvs, schema) = self.get_rdd()
        actual_kvs = kvs.collect()

        expected_kvs = [self.tuple_list, self.tuple_list]

        for actual_tuples, expected_tuples in zip(actual_kvs, expected_kvs):
            for actual, expected in zip(actual_tuples, expected_tuples):
                (actual_tile, actual_extent) = actual
                (expected_tile, expected_extent) = expected

                self.assertTrue((actual_tile['data'] == expected_tile['data']).all())
                self.assertDictEqual(actual_extent, expected_extent)


if __name__ == "__main__":
    unittest.main()
