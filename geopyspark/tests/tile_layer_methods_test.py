from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from pyspark import SparkContext
from geopyspark.geotrellis.tile_layer_methods import TileLayerMethods
from geopyspark.geotrellis.geotiff_rdd import HadoopGeoTiffRDD
from geopyspark.geopycontext import GeoPyContext

import unittest
import pytest
import numpy as np


class TileLayerMethodsTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="metadata-test")
        self.geopysc = GeoPyContext(self.pysc)
        self.methods = TileLayerMethods(self.geopysc)
        self.hadoop_geotiff = HadoopGeoTiffRDD(self.geopysc)

        self.dir_path = geotiff_test_path("all-ones.tif")
        self.rdd = self.hadoop_geotiff.get_spatial(self.dir_path)

        self.value = self.rdd.collect()[0]

        _projected_extent = self.value[0]
        _old_extent = _projected_extent.extent

        self.new_extent = {
            "xmin": _old_extent.xmin,
            "ymin": _old_extent.ymin,
            "xmax": _old_extent.xmax,
            "ymax": _old_extent.ymax
        }

        (_rows, _cols) = self.value[1].shape

        self.layout = {
            "layoutCols": 1,
            "layoutRows": 1,
            "tileCols": _cols,
            "tileRows": _rows
        }

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.geopysc.pysc.stop()
        self.geopysc.pysc._gateway.close()

    def test_cut_tiles(self):
        metadata = self.methods.collect_metadata(self.rdd,
                                                 self.new_extent,
                                                 self.layout,
                                                 epsg_code=self.value[0].epsg_code)

        result = self.methods.cut_tiles(self.rdd,
                                        metadata)

        (key_bounds, tile) = result.collect()[0]

        self.assertEqual([0,0], [key_bounds.col, key_bounds.row])
        self.assertTrue((self.value[1] == tile).all())

    def test_tile_to_layout(self):
        metadata = self.methods.collect_metadata(self.rdd,
                                                 self.new_extent,
                                                 self.layout,
                                                 epsg_code=self.value[0].epsg_code)

        result = self.methods.cut_tiles(self.rdd,
                                        metadata)

        (key_bounds, tile) = result.collect()[0]

        self.assertEqual([0,0], [key_bounds.col, key_bounds.row])
        self.assertTrue((self.value[1] == tile).all())


if __name__ == "__main__":
    unittest.main()
