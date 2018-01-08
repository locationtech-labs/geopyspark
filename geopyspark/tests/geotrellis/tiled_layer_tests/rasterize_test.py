import os
import unittest
import numpy as np
import math

import pytest

from geopyspark.geotrellis.constants import Partitioner
from shapely.geometry import Polygon
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import rasterize


class RasterizeTest(BaseTestClass):
    polygon = Polygon([(0, 11), (11, 11), (11, 0), (0, 0)])
    polygon_collection = [polygon]

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_whole_area(self):
        raster_rdd = rasterize(self.polygon_collection,
                               "EPSG:3857",
                               11,
                               1)

        cells = raster_rdd.to_numpy_rdd().values().first().cells

        for x in cells.flatten().tolist():
            self.assertTrue(math.isnan(x))

    def test_whole_area_rdd(self):
        python_rdd = BaseTestClass.pysc.parallelize(self.polygon_collection)
        raster_rdd = rasterize(python_rdd,
                               "EPSG:3857",
                               11,
                               1)

        cells = raster_rdd.to_numpy_rdd().values().first().cells

        for x in cells.flatten().tolist():
            self.assertTrue(math.isnan(x))

    def test_whole_area_with_spatial_partitioner(self):
        polygon = Polygon([(0, 11), (11, 11), (11, 0), (0, 0)])

        raster_rdd = rasterize([polygon],
                               "EPSG:3857",
                               11,
                               1,
                               partitioner=Partitioner.SPATIAL_PARTITIONER)

        cells = raster_rdd.to_numpy_rdd().first()[1].cells

        for x in cells.flatten().tolist():
            self.assertTrue(math.isnan(x))

    def test_whole_area_integer_crs(self):
        raster_rdd = rasterize(self.polygon_collection,
                               3857,
                               11,
                               1)

        cells = raster_rdd.to_numpy_rdd().values().first().cells

        for x in cells.flatten().tolist():
            self.assertTrue(math.isnan(x))

    def test_whole_area_integer_crs_rdd(self):
        python_rdd = BaseTestClass.pysc.parallelize(self.polygon_collection)
        raster_rdd = rasterize(python_rdd,
                               3857,
                               11,
                               1)

        cells = raster_rdd.to_numpy_rdd().values().first().cells

        for x in cells.flatten().tolist():
            self.assertTrue(math.isnan(x))


if __name__ == "__main__":
    unittest.main()
