from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from pyspark import SparkContext
from geopyspark.geotrellis.tile_layer_metadata import TileLayerMethods
from geopyspark.geotrellis.geotiff_rdd import HadoopGeoTiffRDD
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer

import unittest
import pytest
import json


class TileLayerMetadataTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="metadata-test")
        self.metadata = TileLayerMethods(self.pysc)
        self.hadoop_geotiff = HadoopGeoTiffRDD(self.pysc)

        self.dir_path = geotiff_test_path("all-ones.tif")
        self.options = {'maxTileSize': 256}

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.pysc.stop()
        self.pysc._gateway.close()

    def check_results(self, actual, expected):
        if isinstance(actual, list) and isinstance(expected, list):
            for x,y in zip(actual, expected):
                self.check_results(x, y)
        elif isinstance(actual, dict) and isinstance(expected, dict):
            self.assertDictEqual(actual, expected)
        else:
            self.assertEqual(actual, expected)

    def test_collection(self):
        (rdd, schema) = self.hadoop_geotiff.get_spatial(self.dir_path)
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
                                                schema,
                                                new_extent,
                                                layout,
                                                epsg_code=value[0].epsg_code)

        result_dict = json.loads(result)

        expected = [[result_dict['layoutDefinition']['extent'],
                     result_dict['layoutDefinition']['tileLayout']],
                    result_dict['extent']]

        self.check_results(actual, expected)


if __name__ == "__main__":
    unittest.main()
