import unittest
import os
import pytest

import geopyspark as gps

from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.tests.python_test_utils import geotiff_test_path


class CatalogTest(BaseTestClass):
    uri = geotiff_test_path("srtm_52_11.tif")

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_tiles(self):
        tiles = gps.rasterio.read_windows(self.uri, crs_to_proj4 = lambda n: '+proj=longlat +datum=WGS84 +no_defs ')
        self.assertEqual(len(tiles), 144)

    def test_layer(self):
        to_pretiles = lambda uri: grr.uri_to_pretiles(uri)
        rdd0 = gps.rasterio.get([geotiff_test_path("srtm_52_11.tif")])
        rdd1 = gps.RasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, rdd0)
        self.assertEqual(rdd1.count(), 144)

if __name__ == "__main__":
    unittest.main()
