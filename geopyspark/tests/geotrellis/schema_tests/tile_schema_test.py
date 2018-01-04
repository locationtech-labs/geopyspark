import math
import unittest
import numpy as np

from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.geotrellis import Tile
from geopyspark.geotrellis.protobufserializer import ProtoBufSerializer
from geopyspark.geotrellis.protobufcodecs import tile_decoder, tile_encoder, to_pb_tile
from geopyspark.tests.base_test_class import BaseTestClass


mapped_data_types = {
    'BIT': 0,
    'BYTE': 1,
    'UBYTE': 2,
    'SHORT': 3,
    'USHORT': 4,
    'INT': 5,
    'FLOAT': 6,
    'DOUBLE': 7
}


class ShortTileSchemaTest(BaseTestClass):
    tiles = [
        Tile.from_numpy_array(np.int16([0, 0, 1, 1]).reshape(2, 2), -32768),
        Tile.from_numpy_array(np.int16([1, 2, 3, 4]).reshape(2, 2), -32768),
        Tile.from_numpy_array(np.int16([5, 6, 7, 8]).reshape(2, 2), -32768)
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    tw = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.ShortArrayTileWrapper

    java_rdd = tw.testOut(sc)
    ser = ProtoBufSerializer(tile_decoder, tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        expected_encoded = [to_pb_tile(x) for x in self.collected]

        for actual, expected in zip(self.tiles, expected_encoded):
            cells = actual.cells
            rows, cols = cells.shape

            self.assertEqual(expected.cols, cols)
            self.assertEqual(expected.rows, rows)
            self.assertEqual(expected.cellType.nd, actual.no_data_value)
            self.assertEqual(expected.cellType.dataType, mapped_data_types[actual.cell_type])

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual.cells == expected.cells).all())
            self.assertTrue(actual.cells.dtype == expected.cells.dtype)
            self.assertEqual(actual.cells.shape, actual.cells.shape)


class UShortTileSchemaTest(BaseTestClass):
    tiles = [
        Tile.from_numpy_array(np.uint16([0, 0, 1, 1]).reshape(2, 2), 0),
        Tile.from_numpy_array(np.uint16([1, 2, 3, 4]).reshape(2, 2), 0),
        Tile.from_numpy_array(np.uint16([5, 6, 7, 8]).reshape(2, 2), 0)
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    tw = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.UShortArrayTileWrapper

    java_rdd = tw.testOut(sc)
    ser = ProtoBufSerializer(tile_decoder, tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        expected_encoded = [to_pb_tile(x) for x in self.collected]

        for actual, expected in zip(self.tiles, expected_encoded):
            cells = actual.cells
            rows, cols = cells.shape

            self.assertEqual(expected.cols, cols)
            self.assertEqual(expected.rows, rows)
            self.assertEqual(expected.cellType.nd, actual.no_data_value)
            self.assertEqual(expected.cellType.dataType, mapped_data_types[actual.cell_type])

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual.cells == expected.cells).all())
            self.assertTrue(actual.cells.dtype == expected.cells.dtype)
            self.assertEqual(actual.cells.shape, actual.cells.shape)


class ByteTileSchemaTest(BaseTestClass):
    tiles = [
        Tile.from_numpy_array(np.int8([0, 0, 1, 1]).reshape(2, 2), -128),
        Tile.from_numpy_array(np.int8([1, 2, 3, 4]).reshape(2, 2), -128),
        Tile.from_numpy_array(np.int8([5, 6, 7, 8]).reshape(2, 2), -128)
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    tw = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.ByteArrayTileWrapper

    java_rdd = tw.testOut(sc)
    ser = ProtoBufSerializer(tile_decoder, tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        expected_encoded = [to_pb_tile(x) for x in self.collected]

        for actual, expected in zip(self.tiles, expected_encoded):
            cells = actual.cells
            rows, cols = cells.shape

            self.assertEqual(expected.cols, cols)
            self.assertEqual(expected.rows, rows)
            self.assertEqual(expected.cellType.nd, actual.no_data_value)
            self.assertEqual(expected.cellType.dataType, mapped_data_types[actual.cell_type])

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual.cells == expected.cells).all())
            self.assertTrue(actual.cells.dtype == expected.cells.dtype)
            self.assertEqual(actual.cells.shape, actual.cells.shape)


class UByteTileSchemaTest(BaseTestClass):
    tiles = [
        Tile.from_numpy_array(np.uint8([0, 0, 1, 1]).reshape(2, 2), 0),
        Tile.from_numpy_array(np.uint8([1, 2, 3, 4]).reshape(2, 2), 0),
        Tile.from_numpy_array(np.uint8([5, 6, 7, 8]).reshape(2, 2), 0)
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    tw = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.UByteArrayTileWrapper

    java_rdd = tw.testOut(sc)
    ser = ProtoBufSerializer(tile_decoder, tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        expected_encoded = [to_pb_tile(x) for x in self.collected]

        for actual, expected in zip(self.tiles, expected_encoded):
            cells = actual.cells
            rows, cols = cells.shape

            self.assertEqual(expected.cols, cols)
            self.assertEqual(expected.rows, rows)
            self.assertEqual(expected.cellType.nd, actual.no_data_value)
            self.assertEqual(expected.cellType.dataType, mapped_data_types[actual.cell_type])

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual.cells == expected.cells).all())
            self.assertTrue(actual.cells.dtype == expected.cells.dtype)
            self.assertEqual(actual.cells.shape, actual.cells.shape)


class IntTileSchemaTest(BaseTestClass):
    tiles = [
        Tile.from_numpy_array(np.int32([0, 0, 1, 1]).reshape(2, 2), -2147483648),
        Tile.from_numpy_array(np.int32([1, 2, 3, 4]).reshape(2, 2), -2147483648),
        Tile.from_numpy_array(np.int32([5, 6, 7, 8]).reshape(2, 2), -2147483648)
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    tw = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.IntArrayTileWrapper

    java_rdd = tw.testOut(sc)
    ser = ProtoBufSerializer(tile_decoder, tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        expected_encoded = [to_pb_tile(x) for x in self.collected]

        for actual, expected in zip(self.tiles, expected_encoded):
            cells = actual.cells
            rows, cols = cells.shape

            self.assertEqual(expected.cols, cols)
            self.assertEqual(expected.rows, rows)
            self.assertEqual(expected.cellType.nd, actual.no_data_value)
            self.assertEqual(expected.cellType.dataType, mapped_data_types[actual.cell_type])

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual.cells == expected.cells).all())
            self.assertTrue(actual.cells.dtype == expected.cells.dtype)
            self.assertEqual(actual.cells.shape, actual.cells.shape)


class DoubleTileSchemaTest(BaseTestClass):
    tiles = [
        Tile.from_numpy_array(np.double([0, 0, 1, 1]).reshape(2, 2), True),
        Tile.from_numpy_array(np.double([1, 2, 3, 4]).reshape(2, 2), True),
        Tile.from_numpy_array(np.double([5, 6, 7, 8]).reshape(2, 2), True)
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    tw = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.DoubleArrayTileWrapper

    java_rdd = tw.testOut(sc)
    ser = ProtoBufSerializer(tile_decoder, tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        expected_encoded = [to_pb_tile(x) for x in self.collected]

        for actual, expected in zip(self.tiles, expected_encoded):
            cells = actual.cells
            rows, cols = cells.shape

            self.assertEqual(expected.cols, cols)
            self.assertEqual(expected.rows, rows)
            self.assertEqual(expected.cellType.dataType, mapped_data_types[actual.cell_type])

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual.cells == expected.cells).all())
            self.assertTrue(actual.cells.dtype == expected.cells.dtype)
            self.assertEqual(actual.cells.shape, actual.cells.shape)


class FloatTileSchemaTest(BaseTestClass):
    tiles = [
        Tile.from_numpy_array(np.float32([0, 0, 1, 1]).reshape(2, 2), True),
        Tile.from_numpy_array(np.float32([1, 2, 3, 4]).reshape(2, 2), True),
        Tile.from_numpy_array(np.float32([5, 6, 7, 8]).reshape(2, 2), True)
    ]

    sc = BaseTestClass.pysc._jsc.sc()
    tw = BaseTestClass.pysc._jvm.geopyspark.geotrellis.tests.schemas.FloatArrayTileWrapper

    java_rdd = tw.testOut(sc)
    ser = ProtoBufSerializer(tile_decoder, tile_encoder)

    rdd = RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser))
    collected = rdd.collect()

    def test_encoded_tiles(self):
        expected_encoded = [to_pb_tile(x) for x in self.collected]

        for actual, expected in zip(self.tiles, expected_encoded):
            cells = actual.cells
            rows, cols = cells.shape

            self.assertEqual(expected.cols, cols)
            self.assertEqual(expected.rows, rows)
            self.assertEqual(expected.cellType.dataType, mapped_data_types[actual.cell_type])

    def test_decoded_tiles(self):
        for actual, expected in zip(self.collected, self.tiles):
            self.assertTrue((actual.cells == expected.cells).all())
            self.assertTrue(actual.cells.dtype == expected.cells.dtype)
            self.assertEqual(actual.cells.shape, actual.cells.shape)


if __name__ == "__main__":
    unittest.main()
