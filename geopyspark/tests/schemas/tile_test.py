#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry
from geopyspark.tile import TileArray

import numpy as np
import unittest


# TODO: CLEANUP THESE TESTS TO MAKE IT MORE DRY


class TileSchemaTest(unittest.TestCase):
    pysc = SparkContext(master="local", appName="tile-test")


class ShortTileSchemaTest(TileSchemaTest):
    path = "geopyspark.geotrellis.tests.schemas.ShortArrayTileWrapper"
    java_import(TileSchemaTest.pysc._gateway.jvm, path)

    tiles = [
            TileArray(np.array([0, 0, 1, 1]).reshape(2, 2), -32768),
            TileArray(np.array([1, 2, 3, 4]).reshape(2, 2), -32768),
            TileArray(np.array([5, 6, 7, 8]).reshape(2, 2), -32768)
            ]

    def get_rdd(self):
        sc = TileSchemaTest.pysc._jsc.sc()
        tw = TileSchemaTest.pysc._gateway.jvm.ShortArrayTileWrapper

        tup = tw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tiles(self):
        (tiles, schema) = self.get_rdd()

        return tiles.collect()

    def test_encoded_tiles(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.tile_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': -32768},
                {'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': -32768},
                {'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': -32768}
                ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_tiles(self):
        actual_tiles = self.get_tiles()

        expected_tiles = self.tiles

        for actual, expected in zip(actual_tiles, expected_tiles):
            self.assertTrue((actual == expected).all())


class UShortTileSchemaTest(TileSchemaTest):
    path = "geopyspark.geotrellis.tests.schemas.UShortArrayTileWrapper"
    java_import(TileSchemaTest.pysc._gateway.jvm, path)

    tiles = [
            TileArray(np.array([0, 0, 1, 1]).reshape(2, 2), 0),
            TileArray(np.array([1, 2, 3, 4]).reshape(2, 2), 0),
            TileArray(np.array([5, 6, 7, 8]).reshape(2, 2), 0)
            ]

    def get_rdd(self):
        sc = TileSchemaTest.pysc._jsc.sc()
        tw = TileSchemaTest.pysc._gateway.jvm.UShortArrayTileWrapper

        tup = tw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tiles(self):
        (tiles, schema) = self.get_rdd()

        return tiles.collect()

    def test_encoded_tiles(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.tile_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': 0},
                {'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': 0},
                {'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': 0}
                ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_tiles(self):
        actual_tiles = self.get_tiles()

        expected_tiles = self.tiles

        for actual, expected in zip(actual_tiles, expected_tiles):
            self.assertTrue((actual == expected).all())


class ByteTileSchemaTest(TileSchemaTest):
    path = "geopyspark.geotrellis.tests.schemas.ByteArrayTileWrapper"
    java_import(TileSchemaTest.pysc._gateway.jvm, path)

    tiles = [
            TileArray(np.array(bytearray([0, 0, 1, 1])).reshape(2, 2), -128),
            TileArray(np.array(bytearray([1, 2, 3, 4])).reshape(2, 2), -128),
            TileArray(np.array(bytearray([5, 6, 7, 8])).reshape(2, 2), -128)
            ]

    def get_rdd(self):
        sc = TileSchemaTest.pysc._jsc.sc()
        tw = TileSchemaTest.pysc._gateway.jvm.ByteArrayTileWrapper

        tup = tw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tiles(self):
        (tiles, schema) = self.get_rdd()

        return tiles.collect()

    def test_encoded_tiles(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.tile_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'cols': 2, 'rows': 2, 'cells': bytearray([0, 0, 1, 1]), 'noDataValue': -128},
                {'cols': 2, 'rows': 2, 'cells': bytearray([1, 2, 3, 4]), 'noDataValue': -128},
                {'cols': 2, 'rows': 2, 'cells': bytearray([5, 6, 7, 8]), 'noDataValue': -128}
                ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_tiles(self):
        actual_tiles = self.get_tiles()

        expected_tiles = self.tiles

        for actual, expected in zip(actual_tiles, expected_tiles):
            self.assertTrue((actual == expected).all())


class UByteTileSchemaTest(TileSchemaTest):
    path = "geopyspark.geotrellis.tests.schemas.UByteArrayTileWrapper"
    java_import(TileSchemaTest.pysc._gateway.jvm, path)

    tiles = [
            TileArray(np.array(bytearray([0, 0, 1, 1])).reshape(2, 2), 0),
            TileArray(np.array(bytearray([1, 2, 3, 4])).reshape(2, 2), 0),
            TileArray(np.array(bytearray([5, 6, 7, 8])).reshape(2, 2), 0)
            ]

    def get_rdd(self):
        sc = TileSchemaTest.pysc._jsc.sc()
        tw = TileSchemaTest.pysc._gateway.jvm.UByteArrayTileWrapper

        tup = tw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tiles(self):
        (tiles, schema) = self.get_rdd()

        return tiles.collect()

    def test_encoded_tiles(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.tile_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'cols': 2, 'rows': 2, 'cells': bytearray([0, 0, 1, 1]), 'noDataValue': 0},
                {'cols': 2, 'rows': 2, 'cells': bytearray([1, 2, 3, 4]), 'noDataValue': 0},
                {'cols': 2, 'rows': 2, 'cells': bytearray([5, 6, 7, 8]), 'noDataValue': 0}
                ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_tiles(self):
        actual_tiles = self.get_tiles()

        expected_tiles = self.tiles

        for actual, expected in zip(actual_tiles, expected_tiles):
            self.assertTrue((actual == expected).all())


class IntTileSchemaTest(TileSchemaTest):
    path = "geopyspark.geotrellis.tests.schemas.IntArrayTileWrapper"
    java_import(TileSchemaTest.pysc._gateway.jvm, path)

    tiles = [
            TileArray(np.array([0, 0, 1, 1]).reshape(2, 2), -2147483648),
            TileArray(np.array([1, 2, 3, 4]).reshape(2, 2), -2147483648),
            TileArray(np.array([5, 6, 7, 8]).reshape(2, 2), -2147483648)
            ]

    def get_rdd(self):
        sc = TileSchemaTest.pysc._jsc.sc()
        tw = TileSchemaTest.pysc._gateway.jvm.IntArrayTileWrapper

        tup = tw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tiles(self):
        (tiles, schema) = self.get_rdd()

        return tiles.collect()

    def test_encoded_tiles(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.tile_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': -2147483648},
                {'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': -2147483648},
                {'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': -2147483648}
                ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_tiles(self):
        actual_tiles = self.get_tiles()

        expected_tiles = self.tiles

        for actual, expected in zip(actual_tiles, expected_tiles):
            self.assertTrue((actual == expected).all())


class DoubleTileSchemaTest(TileSchemaTest):
    path = "geopyspark.geotrellis.tests.schemas.DoubleArrayTileWrapper"
    java_import(TileSchemaTest.pysc._gateway.jvm, path)

    tiles = [
            TileArray(np.array([0, 0, 1, 1]).reshape(2, 2), True),
            TileArray(np.array([1, 2, 3, 4]).reshape(2, 2), True),
            TileArray(np.array([5, 6, 7, 8]).reshape(2, 2), True)
            ]

    def get_rdd(self):
        sc = TileSchemaTest.pysc._jsc.sc()
        tw = TileSchemaTest.pysc._gateway.jvm.DoubleArrayTileWrapper

        tup = tw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tiles(self):
        (tiles, schema) = self.get_rdd()

        return tiles.collect()

    def test_encoded_tiles(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.tile_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': True},
                {'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': True},
                {'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': True}
                ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_tiles(self):
        actual_tiles = self.get_tiles()

        expected_tiles = self.tiles

        for actual, expected in zip(actual_tiles, expected_tiles):
            self.assertTrue((actual == expected).all())


class FloatTileSchemaTest(TileSchemaTest):
    path = "geopyspark.geotrellis.tests.schemas.FloatArrayTileWrapper"
    java_import(TileSchemaTest.pysc._gateway.jvm, path)

    tiles = [
            TileArray(np.array([0, 0, 1, 1]).reshape(2, 2), True),
            TileArray(np.array([1, 2, 3, 4]).reshape(2, 2), True),
            TileArray(np.array([5, 6, 7, 8]).reshape(2, 2), True)
            ]

    def get_rdd(self):
        sc = TileSchemaTest.pysc._jsc.sc()
        tw = TileSchemaTest.pysc._gateway.jvm.FloatArrayTileWrapper

        tup = tw.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tiles(self):
        (tiles, schema) = self.get_rdd()

        return tiles.collect()

    def test_encoded_tiles(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: AvroRegistry.tile_encoder(s))

        actual_encoded = encoded.collect()

        expected_encoded = [
                {'cols': 2, 'rows': 2, 'cells': [0, 0, 1, 1], 'noDataValue': True},
                {'cols': 2, 'rows': 2, 'cells': [1, 2, 3, 4], 'noDataValue': True},
                {'cols': 2, 'rows': 2, 'cells': [5, 6, 7, 8], 'noDataValue': True}
                ]

        self.assertEqual(actual_encoded, expected_encoded)

    def test_decoded_tiles(self):
        actual_tiles = self.get_tiles()

        expected_tiles = self.tiles

        for actual, expected in zip(actual_tiles, expected_tiles):
            self.assertTrue((actual == expected).all())


if __name__ == "__main__":
    unittest.main()
