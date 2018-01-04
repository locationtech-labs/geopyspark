import unittest
import datetime
import pytest

from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.geotrellis import SpaceTimeKey
from geopyspark.geotrellis.protobuf import keyMessages_pb2
from geopyspark.geotrellis.protobufserializer import ProtoBufSerializer
from geopyspark.geotrellis.protobufcodecs import (spatial_key_decoder,
                                                  spatial_key_encoder,
                                                  space_time_key_decoder,
                                                  space_time_key_encoder,
                                                  _convert_to_unix_time)
from geopyspark.tests.base_test_class import BaseTestClass


class SpatialKeySchemaTest(BaseTestClass):
    expected_keys = {'col': 7, 'row': 3}

    sc = BaseTestClass.pysc._jsc.sc()
    ew = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.SpatialKeyWrapper

    java_rdd = ew.testOut(sc)
    ser = ProtoBufSerializer(spatial_key_decoder,
                             spatial_key_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.first()._asdict()

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def result_checker(self, actual_keys, expected_keys):
        self.assertDictEqual(actual_keys, expected_keys)

    def test_encoded_keyss(self):
        actual_encoded = [spatial_key_encoder(x) for x in self.rdd.collect()]
        proto_spatial_key = keyMessages_pb2.ProtoSpatialKey()

        proto_spatial_key.col = 7
        proto_spatial_key.row = 3

        expected_encoded = proto_spatial_key.SerializeToString()

        self.assertEqual(actual_encoded[0], expected_encoded)

    def test_decoded_extents(self):
        self.assertDictEqual(self.collected, self.expected_keys)


class SpaceTimeKeySchemaTest(BaseTestClass):
    time = datetime.datetime.strptime("2016-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')

    expected_keys = [
        SpaceTimeKey(7, 3, time)._asdict(),
        SpaceTimeKey(9, 4, time)._asdict(),
        SpaceTimeKey(11, 5, time)._asdict(),
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    ew = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.SpaceTimeKeyWrapper

    java_rdd = ew.testOut(sc)
    ser = ProtoBufSerializer(space_time_key_decoder,
                             space_time_key_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = [stk._asdict() for stk in rdd.collect()]

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def result_checker(self, actual_keys, expected_keys):
        for actual, expected in zip(actual_keys, expected_keys):
            self.assertDictEqual(actual, expected)

    def test_encoded_keyss(self):
        expected_encoded = [space_time_key_encoder(x) for x in self.rdd.collect()]
        actual_encoded = []

        for x in self.expected_keys:
            proto_space_time_key = keyMessages_pb2.ProtoSpaceTimeKey()

            proto_space_time_key.col = x['col']
            proto_space_time_key.row = x['row']
            proto_space_time_key.instant = _convert_to_unix_time(x['instant'])

            actual_encoded.append(proto_space_time_key.SerializeToString())

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)
        expected_encoded = [space_time_key_encoder(x) for x in self.rdd.collect()]
        actual_encoded = []

        for x in self.expected_keys:
            proto_space_time_key = keyMessages_pb2.ProtoSpaceTimeKey()

            proto_space_time_key.col = x['col']
            proto_space_time_key.row = x['row']
            proto_space_time_key.instant = _convert_to_unix_time(x['instant'])

            actual_encoded.append(proto_space_time_key.SerializeToString())

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_extents(self):
        self.result_checker(self.collected, self.expected_keys)


if __name__ == "__main__":
    unittest.main()
