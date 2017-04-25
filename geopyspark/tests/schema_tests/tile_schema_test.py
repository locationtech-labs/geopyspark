import unittest
import numpy as np

from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry
from geopyspark.tests.base_test_class import BaseTestClass


class ShortTileSchemaTest(BaseTestClass):
    tiles = [
        {'data': np.array([0, 0, 1, 1]).reshape(2, 2), 'no_data_value': -32768},
        {'data': np.array([1, 2, 3, 4]).reshape(2, 2), 'no_data_value': -32768},
        {'data': np.array([5, 6, 7, 8]).reshape(2, 2), 'no_data_value': -32768}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    tw = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.ShortArrayTileWrapper

    tup = tw.testOut(sc)
    java_rdd = tup._1()
    ser = AvroSerializer(tup._2(), AvroRegistry.tile_decoder, AvroRegistry.tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        encoded = self.rdd.map(lambda s: AvroRegistry.tile_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': -32768}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': -32768}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': -32768}]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual['data'] == expected['data']).all())


class UShortTileSchemaTest(BaseTestClass):
    tiles = [
        {'data': np.array([0, 0, 1, 1]).reshape(2, 2), 'no_data_value': 0},
        {'data': np.array([1, 2, 3, 4]).reshape(2, 2), 'no_data_value': 0},
        {'data': np.array([5, 6, 7, 8]).reshape(2, 2), 'no_data_value': 0}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    tw = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.UShortArrayTileWrapper

    tup = tw.testOut(sc)
    java_rdd = tup._1()
    ser = AvroSerializer(tup._2(), AvroRegistry.tile_decoder, AvroRegistry.tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        encoded = self.rdd.map(lambda s: AvroRegistry.tile_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': 0}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': 0}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': 0}]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual['data'] == expected['data']).all())


class ByteTileSchemaTest(BaseTestClass):
    tiles = [
        {'data': np.array([0, 0, 1, 1]).reshape(2, 2), 'no_data_value': -128},
        {'data': np.array([1, 2, 3, 4]).reshape(2, 2), 'no_data_value': -128},
        {'data': np.array([5, 6, 7, 8]).reshape(2, 2), 'no_data_value': -128}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    tw = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.ByteArrayTileWrapper

    tup = tw.testOut(sc)
    java_rdd = tup._1()
    ser = AvroSerializer(tup._2(), AvroRegistry.tile_decoder, AvroRegistry.tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        encoded = self.rdd.map(lambda s: AvroRegistry.tile_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [{'cols': 2, 'rows': 2, 'cells': bytearray([0, 0, 1, 1]), 'noDataValue': -128}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': bytearray([1, 2, 3, 4]), 'noDataValue': -128}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': bytearray([5, 6, 7, 8]), 'noDataValue': -128}]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual['data'] == expected['data']).all())


class UByteTileSchemaTest(BaseTestClass):
    tiles = [
        {'data': np.array([0, 0, 1, 1]).reshape(2, 2), 'no_data_value': 0},
        {'data': np.array([1, 2, 3, 4]).reshape(2, 2), 'no_data_value': 0},
        {'data': np.array([5, 6, 7, 8]).reshape(2, 2), 'no_data_value': 0}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    tw = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.UByteArrayTileWrapper

    tup = tw.testOut(sc)
    java_rdd = tup._1()
    ser = AvroSerializer(tup._2(), AvroRegistry.tile_decoder, AvroRegistry.tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        encoded = self.rdd.map(lambda s: AvroRegistry.tile_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [{'cols': 2, 'rows': 2, 'cells': bytearray([0, 0, 1, 1]), 'noDataValue': 0}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': bytearray([1, 2, 3, 4]), 'noDataValue': 0}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': bytearray([5, 6, 7, 8]), 'noDataValue': 0}]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual['data'] == expected['data']).all())


class IntTileSchemaTest(BaseTestClass):
    tiles = [
        {'data': np.array([0, 0, 1, 1]).reshape(2, 2), 'no_data_value': -2147483648},
        {'data': np.array([1, 2, 3, 4]).reshape(2, 2), 'no_data_value': -2147483648},
        {'data': np.array([5, 6, 7, 8]).reshape(2, 2), 'no_data_value': -2147483648}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    tw = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.IntArrayTileWrapper

    tup = tw.testOut(sc)
    java_rdd = tup._1()
    ser = AvroSerializer(tup._2(), AvroRegistry.tile_decoder, AvroRegistry.tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        encoded = self.rdd.map(lambda s: AvroRegistry.tile_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': -2147483648}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': -2147483648}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': -2147483648}]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual['data'] == expected['data']).all())


class DoubleTileSchemaTest(BaseTestClass):
    tiles = [
        {'data': np.array([0, 0, 1, 1]).reshape(2, 2), 'no_data_value': True},
        {'data': np.array([1, 2, 3, 4]).reshape(2, 2), 'no_data_value': True},
        {'data': np.array([5, 6, 7, 8]).reshape(2, 2), 'no_data_value': True}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    tw = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.DoubleArrayTileWrapper

    tup = tw.testOut(sc)
    java_rdd = tup._1()
    ser = AvroSerializer(tup._2(), AvroRegistry.tile_decoder, AvroRegistry.tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        encoded = self.rdd.map(lambda s: AvroRegistry.tile_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': True}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': True}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': True}]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual['data'] == expected['data']).all())


class FloatTileSchemaTest(BaseTestClass):
    tiles = [
        {'data': np.array([0, 0, 1, 1]).reshape(2, 2), 'no_data_value': True},
        {'data': np.array([1, 2, 3, 4]).reshape(2, 2), 'no_data_value': True},
        {'data': np.array([5, 6, 7, 8]).reshape(2, 2), 'no_data_value': True}
    ]

    sc = BaseTestClass.geopysc.pysc._jsc.sc()
    tw = BaseTestClass.geopysc.pysc._jvm.geopyspark.geotrellis.tests.schemas.FloatArrayTileWrapper

    tup = tw.testOut(sc)
    java_rdd = tup._1()
    ser = AvroSerializer(tup._2(), AvroRegistry.tile_decoder, AvroRegistry.tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.geopysc.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        encoded = self.rdd.map(lambda s: AvroRegistry.tile_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': True}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': True}]},
            {'bands': [{'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': True}]}
        ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual['data'] == expected['data']).all())


if __name__ == "__main__":
    unittest.main()
