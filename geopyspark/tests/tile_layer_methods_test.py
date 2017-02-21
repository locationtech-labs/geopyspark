from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from pyspark import SparkContext
from geopyspark.geotrellis.tile_layer_methods import TileLayerMethods
from geopyspark.geotrellis.geotiff_rdd import HadoopGeoTiffRDD
from geopyspark.tests.base_test_class import BaseTestClass

import unittest
import pytest
import numpy as np


class TileLayerMethodsTest(BaseTestClass):
    methods = TileLayerMethods(BaseTestClass.geopysc)
    hadoop_geotiff = HadoopGeoTiffRDD(BaseTestClass.geopysc)

    dir_path = geotiff_test_path("all-ones.tif")
    rdd = hadoop_geotiff.get_spatial(dir_path)

    value = rdd.collect()[0]

    _projected_extent = value[0]
    _old_extent = _projected_extent.extent

    new_extent = {
        "xmin": _old_extent.xmin,
        "ymin": _old_extent.ymin,
        "xmax": _old_extent.xmax,
        "ymax": _old_extent.ymax
    }

    (_rows, _cols) = value[1].shape

    layout = {
        "layoutCols": 1,
        "layoutRows": 1,
        "tileCols": _cols,
        "tileRows": _rows
    }

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

    def test_merge(self):
        result = self.methods.merge(self.rdd, self.rdd)

        (projected_extent, tile) = result.collect()[0]

        self.assertEqual(self.value[0], projected_extent)
        self.assertTrue((self.value[1] == tile).all())


if __name__ == "__main__":
    unittest.main()
