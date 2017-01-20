#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from python_test_utils import *
from os import walk, path

import rasterio
import unittest


class GeoTiffIOTest(unittest.TestCase):
    pysc = SparkContext(master="local", appName="hadoop-geotiff-test")
    sc = pysc._jsc.sc()

    java_import(pysc._gateway.jvm,
            "geopyspark.geotrellis.io.hadoop.HadoopGeoTiffRDDWrapper")

    java_import(pysc._gateway.jvm,
            "geopyspark.geotrellis.io.hadoop.HadoopGeoTiffRDDOptions")

    hadoop_wrapper = pysc._gateway.jvm.HadoopGeoTiffRDDWrapper
    geotiff_rdd_options = pysc._gateway.jvm.HadoopGeoTiffRDDOptions

    def get_filepaths(self, dir_path):
        files = []

        for (fd, dn, filenames) in walk(dir_path):
            files.extend(filenames)

        return [path.join(dir_path, x) for x in files]

    def read_geotiff_rasterio(self, paths, windowed):
        rasterio_tiles = []

        windows = [((0, 256), (0, 256)),
                ((256, 512), (0, 256)),
                ((0, 256), (256, 512)),
                ((256, 512), (256, 512))]

        for f in paths:
            with rasterio.open(f) as src:
                if not windowed:
                    rasterio_tiles.append(src.read())
                else:
                    for window in windows:
                        rasterio_tiles.append(src.read(window=window))

        return rasterio_tiles


class Singleband(GeoTiffIOTest):
    dir_path = test_path("one-month-tiles/")

    def read_singleband_geotrellis(self, options=None):
        if options is None:
            tup = self.hadoop_wrapper.spatial(self.dir_path, self.sc)
        else:
            tup = self.hadoop_wrapper.spatial(self.dir_path, options, self.sc)

        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        rdd = RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser))
        tiles = [x[1] for x in rdd.collect()]

        return tiles

    def test_whole_tiles(self):

        geotrellis_tiles = self.read_singleband_geotrellis()

        file_paths = self.get_filepaths(self.dir_path)
        rasterio_tiles = self.read_geotiff_rasterio(file_paths, False)

        for x, y in zip(geotrellis_tiles, rasterio_tiles):
            self.assertTrue((x == y).all())


    def windowed_result_checker(self, windowed_tiles):
        self.assertEqual(len(windowed_tiles), 24)

    def test_windowed_tiles(self):
        options = {
                'maxTileSize': 256,
                }

        geotrellis_tiles = self.read_singleband_geotrellis(options)

        file_paths = self.get_filepaths(self.dir_path)
        rasterio_tiles = self.read_geotiff_rasterio(file_paths, True)

        self.windowed_result_checker(geotrellis_tiles)

        for x, y in zip(geotrellis_tiles, rasterio_tiles):
            self.assertTrue((x == y).all())


class Multiband(GeoTiffIOTest, unittest.TestCase):
    dir_path = test_path("one-month-tiles-multiband/")

    def read_multiband_geotrellis(self, options=None):
        if options is None:
            tup = self.hadoop_wrapper.spatialMultiband(self.dir_path, self.sc)
        else:
            tup = self.hadoop_wrapper.spatialMultiband(self.dir_path, options, self.sc)

        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        rdd = RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser))
        collection = rdd.collect()

        tiles = [x[1] for x in collection]

        return tiles

    def test_whole_tiles(self):
        geotrellis_tiles = self.read_multiband_geotrellis()

        file_paths = self.get_filepaths(self.dir_path)
        rasterio_tiles = self.read_geotiff_rasterio(file_paths, False)

        for x, y in zip(geotrellis_tiles, rasterio_tiles):
            self.assertTrue((x == y).all())

    def windowed_result_checker(self, windowed_tiles):
        self.assertEqual(len(windowed_tiles), 4)

    def test_windowed_tiles(self):
        options = {
                'maxTileSize': 256,
                }

        geotrellis_tiles = self.read_multiband_geotrellis(options=options)

        file_paths = self.get_filepaths(self.dir_path)
        rasterio_tiles = self.read_geotiff_rasterio(file_paths, True)

        self.windowed_result_checker(geotrellis_tiles)

        for x, y in zip(geotrellis_tiles, rasterio_tiles):
            self.assertTrue((x == y).all())


if __name__ == "__main__":
    unittest.main()
