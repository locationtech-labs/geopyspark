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


class MultibandSchemaTest(BaseTestClass):
    path = "geopyspark.geotrellis.tests.schemas.ArrayMultibandTileWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    arr = np.array(bytearray([0, 0, 1, 1])).reshape(2, 2)
    no_data = -128
    arr_dict = {'data': arr, 'no_data_value': no_data}
    band_dicts = [arr_dict, arr_dict, arr_dict]

    bands = [arr, arr, arr]
    multiband_tile = np.array(bands)
    multiband_dict = {'data': multiband_tile, 'no_data_value': no_data}

    def get_rdd(self):
        sc = BaseTestClass.pysc._jsc.sc()
        mw = BaseTestClass.pysc._gateway.jvm.ArrayMultibandTileWrapper

        tup = mw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema,
                             AvroRegistry.multiband_decoder,
                             AvroRegistry.multiband_encoder)

        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def get_multibands(self):
        (multibands, schema) = self.get_rdd()

        return multibands.collect()

    def test_encoded_multibands(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: AvroRegistry.multiband_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [AvroRegistry.tile_encoder(x) for x in self.band_dicts]},
            {'bands': [AvroRegistry.tile_encoder(x) for x in self.band_dicts]},
            {'bands': [AvroRegistry.tile_encoder(x) for x in self.band_dicts]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_multibands(self):
        actual_multibands = self.get_multibands()

        expected_multibands = [
            self.multiband_dict,
            self.multiband_dict,
            self.multiband_dict
        ]

        for actual, expected in zip(actual_multibands, expected_multibands):
            self.assertTrue((actual['data'] == expected['data']).all())


if __name__ == "__main__":
    unittest.main()
