from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from pyspark import SparkContext
from geopyspark.geotrellis.tile_layer_metadata import TileLayerMethods
from geopyspark.geotrellis.geotiff_rdd import HadoopGeoTiffRDD
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.geopycontext import GeoPyContext

import unittest
import pytest
import json


class TileLayerMetadataTest(unittest.TestCase):
    def setUp(self):
        pysc = SparkContext(master="local[*]", appName="metadata-test")
        self.geopysc = GeoPyContext(pysc)
        self.metadata = TileLayerMethods(self.geopysc)
        self.hadoop_geotiff = HadoopGeoTiffRDD(self.geopysc)

        self.dir_path = geotiff_test_path("all-ones.tif")
        self.options = {'maxTileSize': 256}

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.geopysc.stop()
        self.geopysc.close_gateway()

    def check_results(self, actual, expected):
        if isinstance(actual, list) and isinstance(expected, list):
            for x,y in zip(actual, expected):
                self.check_results(x, y)
        elif isinstance(actual, dict) and isinstance(expected, dict):
            self.assertDictEqual(actual, expected)
        else:
            self.assertEqual(actual, expected)

    def test_collection(self):
        rdd = self.hadoop_geotiff.get_spatial(self.dir_path)
        schema = rdd.schema
        value = rdd.collect()[0]

        projected_extent = value[0]
        old_extent = projected_extent.extent

        new_extent = {
            "xmin": old_extent.xmin,
            "ymin": old_extent.ymin,
            "xmax": old_extent.xmax,
            "ymax": old_extent.ymax
        }

        (rows, cols) = value[1].shape

        layout = {
            "layoutCols": 1,
            "layoutRows": 1,
            "tileCols": cols,
            "tileRows": rows
        }

        actual = [[new_extent, layout], new_extent]

        result = self.metadata.collect_metadata(rdd,
                                                new_extent,
                                                layout,
                                                epsg_code=value[0].epsg_code)

        expected = [[result['layoutDefinition']['extent'],
                     result['layoutDefinition']['tileLayout']],
                    result['extent']]

        self.check_results(actual, expected)

    def test_failure(self):
        rdd = self.hadoop_geotiff.get_spatial(self.dir_path)
        schema = rdd.schema
        value = rdd.collect()[0]

        projected_extent = value[0]
        old_extent = projected_extent.extent

        new_extent = {
            "xmin": old_extent.xmax,
            "ymin": old_extent.ymin,
            "xmax": old_extent.ymin,
            "ymax": old_extent.ymax
        }

        (cols, rows) = value[1].shape

        layout = {
            "layoutCols": 1,
            "layoutRows": 1,
            "tileCols": cols,
            "tileRows": rows
        }

        self.assertRaises(Exception,
                          self.metadata.collect_metadata,
                          rdd,
                          new_extent,
                          layout,
                          epsg_code=value[0].epsg_code)


if __name__ == "__main__":
    unittest.main()
