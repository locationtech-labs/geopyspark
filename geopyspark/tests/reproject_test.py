import os
import unittest

from geopyspark.tests.python_test_utils import check_directory, geotiff_test_path
from geopyspark.geotrellis.tile_layer import reproject, collect_metadata, tile_to_layout
from geopyspark.geotrellis.geotiff_rdd import geotiff_rdd
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import SPATIAL


check_directory()


class ReprojectTest(BaseTestClass):
    dir_path = geotiff_test_path("all-ones.tif")

    rdd = geotiff_rdd(BaseTestClass.geopysc, SPATIAL, dir_path)
    value = rdd.collect()[0]

    extent = value[0]['extent']

    (_, _rows, _cols) = value[1]['data'].shape

    layout = {
        "layoutCols": 1,
        "layoutRows": 1,
        "tileCols": _cols,
        "tileRows": _rows
    }

    metadata = collect_metadata(BaseTestClass.geopysc,
                                SPATIAL,
                                rdd,
                                extent,
                                layout)
    crs = metadata['crs']

    laid_out_rdd = tile_to_layout(BaseTestClass.geopysc,
                                  SPATIAL,
                                  rdd,
                                  metadata)

    def test_same_crs(self):
        (_, new_metadata) = reproject(BaseTestClass.geopysc,
                                      SPATIAL,
                                      self.laid_out_rdd,
                                      self.metadata,
                                      "EPSG:4326")

        self.assertEqual(self.crs, new_metadata['crs'])

    def test_different_crs(self):
        (_, new_metadata) = reproject(BaseTestClass.geopysc,
                                      SPATIAL,
                                      self.laid_out_rdd,
                                      self.metadata,
                                      "EPSG:4324")

        actual = '+proj=longlat +datum=WGS84 +no_defs '

        self.assertEqual(actual, new_metadata['crs'])


if __name__ == "__main__":
    unittest.main()
