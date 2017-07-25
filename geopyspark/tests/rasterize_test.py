import os
import unittest
import numpy as np
import math

import pytest

from geopyspark.geotrellis import Extent
from shapely.geometry import Polygon
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import rasterize


class RasterizeTest(BaseTestClass):
    extent = Extent(0.0, 0.0, 11.0, 11.0)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_whole_area(self):
        polygon = Polygon([(0, 11), (11, 11), (11, 0), (0, 0)])

        raster_rdd = rasterize([polygon],
                               "EPSG:3857",
                               11,
                               1)

        cells = raster_rdd.to_numpy_rdd().first()[1].cells

        for x in cells.flatten().tolist():
            self.assertTrue(math.isnan(x))

    def test_whole_area_integer_crs(self):
        polygon = Polygon([(0, 11), (11, 11), (11, 0), (0, 0)])

        raster_rdd = rasterize([polygon],
                               3857,
                               11,
                               1)

        cells = raster_rdd.to_numpy_rdd().first()[1].cells

        for x in cells.flatten().tolist():
            self.assertTrue(math.isnan(x))

if __name__ == "__main__":
    unittest.main()
