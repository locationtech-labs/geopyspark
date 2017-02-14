from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from pyspark import SparkContext
from geopyspark.geotrellis.tile_layer_metadata import TileLayerMetadata
from geopyspark.geotrellis.geotiff_rdd import HadoopGeoTiffRDD
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer

import unittest
import pytest


class TileLayerMetadataTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="metadata-test")
        self.metadata = TileLayerMetadata(self.pysc)
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

        actual = [value[1].dtype.name, layout, new_extent]

        result = self.metadata.collect_python_metadata(rdd,
                                                       schema,
                                                       new_extent,
                                                       layout,
                                                       epsg_code=value[0].epsg_code)

        returned_layout_extent = result['layout'][0]
        layout_java_object = result['layout'][1]

        returned_extent = {
            'xmin': returned_layout_extent['xmin'],
            'ymin': returned_layout_extent['ymin'],
            'xmax': returned_layout_extent['xmax'],
            'ymax': returned_layout_extent['ymax']
        }

        returned_layout = {
            'layoutCols': layout_java_object['layoutCols'],
            'layoutRows': layout_java_object['layoutRows'],
            'tileCols': layout_java_object['tileCols'],
            'tileRows': layout_java_object['tileRows']
        }

        expected = [result['cellType'], returned_layout, returned_extent]

        self.check_results(actual, expected)


if __name__ == "__main__":
    unittest.main()
