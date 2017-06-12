import unittest
import pytest

from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.protobufserializer import ProtoBufSerializer
from geopyspark.protobufregistry import ProtoBufRegistry
from geopyspark.tests.base_test_class import BaseTestClass


class SpatialKeySchemaTest(BaseTestClass):
    expected_keys = {'col': 7, 'row': 3}

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    ew = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.SpatialKeyWrapper

    java_rdd = ew.testOut(sc)
    ser = ProtoBufSerializer(ProtoBufRegistry.spatial_key_decoder,
                             ProtoBufRegistry.spatial_key_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.first()._asdict()

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.geopysc.pysc._gateway.close()

    def result_checker(self, actual_keys, expected_keys):
        self.assertDictEqual(actual_keys, expected_keys)

    '''
    def test_encoded_keyss(self):
        encoded = self.rdd.map(lambda s: s)
        actual_encoded = encoded.first()

        self.result_checker(actual_encoded, self.expected_keys)
    '''

    def test_decoded_extents(self):
        self.assertDictEqual(self.collected, self.expected_keys)


class SpaceTimeKeySchemaTest(BaseTestClass):
    expected_keys = [
        {'col': 7, 'row': 3, 'instant': 5},
        {'col': 9, 'row': 4, 'instant': 10},
        {'col': 11, 'row': 5, 'instant': 15}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    ew = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.SpaceTimeKeyWrapper

    java_rdd = ew.testOut(sc)
    ser = ProtoBufSerializer(ProtoBufRegistry.space_time_key_decoder,
                             ProtoBufRegistry.space_time_key_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = [stk._asdict() for stk in rdd.collect()]

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.geopysc.pysc._gateway.close()

    def result_checker(self, actual_keys, expected_keys):
        for actual, expected in zip(actual_keys, expected_keys):
            self.assertDictEqual(actual, expected)

    '''
    def test_encoded_keyss(self):
        encoded = self.rdd.map(lambda s: s)
        actual_encoded = encoded.collect()

        self.result_checker(actual_encoded, self.expected_keys)
    '''

    def test_decoded_extents(self):
        self.result_checker(self.collected, self.expected_keys)


if __name__ == "__main__":
    unittest.main()
