import sys
import math
import numpy as np
import pytest
import unittest

from geopyspark.geotrellis.rdd import RasterRDD
from geopyspark.geotrellis.constants import SPATIAL, NODATAINT
from geopyspark.tests.base_test_class import BaseTestClass


class ReclassifyTest(BaseTestClass):
    epsg_code = 3857
    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 10.0, 'ymax': 10.0}

    projected_extent = {'extent': extent, 'epsg': epsg_code}

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.geopysc.pysc._gateway.close()

    def test_all_zeros(self):
        arr = np.zeros((1, 16, 16))
        tile = {'data': arr, 'no_data_value': -500}

        rdd = BaseTestClass.geopysc.pysc.parallelize([(self.projected_extent, tile)])
        raster_rdd = RasterRDD.from_numpy_rdd(BaseTestClass.geopysc, SPATIAL, rdd)

        value_map = {0: 1}

        result = raster_rdd.reclassify(value_map, int).to_numpy_rdd().first()[1]['data']

        self.assertTrue((result == 1).all())

    def test_various_values(self):
        arr = np.array([[[1, 1, 1, 1],
                         [2, 2, 2, 2],
                         [3, 3, 3, 3],
                         [4, 4, 4, 4]]], dtype=int)
        tile = {'data': arr, 'no_data_value': -500}

        rdd = BaseTestClass.geopysc.pysc.parallelize([(self.projected_extent, tile)])
        raster_rdd = RasterRDD.from_numpy_rdd(BaseTestClass.geopysc, SPATIAL, rdd)

        value_map = {1: 10, (2, 3): 17}

        result = raster_rdd.reclassify(value_map, int).to_numpy_rdd().first()[1]['data']

        expected = np.array([[[10, 10, 10, 10],
                              [17, 17, 17, 17],
                              [17, 17, 17, 17],
                              [4, 4, 4, 4]]], dtype=int)

        self.assertTrue((result == expected).all())

    def test_ranges(self):
        arr = np.array([[[1, 1, 1, 1],
                         [2, 2, 2, 2],
                         [3, 3, 3, 3],
                         [4, 4, 4, 4]]], dtype=int)
        tile = {'data': arr, 'no_data_value': -500}

        rdd = BaseTestClass.geopysc.pysc.parallelize([(self.projected_extent, tile)])
        raster_rdd = RasterRDD.from_numpy_rdd(BaseTestClass.geopysc, SPATIAL, rdd)

        value_map = {range(1, 4): 20}

        result = raster_rdd.reclassify(value_map, int).to_numpy_rdd().first()[1]['data']

        expected = np.array([[[20, 20, 20, 20],
                              [20, 20, 20, 20],
                              [20, 20, 20, 20],
                              [4, 4, 4, 4]]], dtype=int)

        self.assertTrue((result == expected).all())

    def test_multibands(self):
        arr = np.array([[[1, 1, 1, 1]],
                        [[2, 2, 2, 2]],
                        [[3, 3, 3, 3]],
                        [[4, 4, 4, 4]]], dtype=int)
        tile = {'data': arr, 'no_data_value': -500}

        rdd = BaseTestClass.geopysc.pysc.parallelize([(self.projected_extent, tile)])
        raster_rdd = RasterRDD.from_numpy_rdd(BaseTestClass.geopysc, SPATIAL, rdd)

        value_map = {(1, 3): 10, (2, 4): 20}

        result = raster_rdd.reclassify(value_map, int).to_numpy_rdd().first()[1]['data']

        expected = np.array([[[10, 10, 10, 10]],
                             [[20, 20, 20, 20]],
                             [[10, 10, 10, 10]],
                             [[20, 20, 20, 20]]], dtype=int)

        self.assertTrue((result == expected).all())

    def test_no_data_ints(self):
        arr = np.zeros((1, 16, 16))
        tile = {'data': arr, 'no_data_value': NODATAINT}

        rdd = BaseTestClass.geopysc.pysc.parallelize([(self.projected_extent, tile)])
        raster_rdd = RasterRDD.from_numpy_rdd(BaseTestClass.geopysc, SPATIAL, rdd)

        value_map = {0: NODATAINT}

        result = raster_rdd.reclassify(value_map, int).to_numpy_rdd().first()[1]['data']

        self.assertTrue((result == NODATAINT).all())

    def test_no_data_floats(self):
        arr = np.array([[[0.0, 0.0, 0.0, 0.0],
                         [0.0, 0.0, 0.0, 0.0],
                         [0.0, 0.0, 0.0, 0.0],
                         [0.0, 0.0, 0.0, 0.0]]], dtype=float)
        tile = {'data': arr, 'no_data_value': float('nan')}

        rdd = BaseTestClass.geopysc.pysc.parallelize([(self.projected_extent, tile)])
        raster_rdd = RasterRDD.from_numpy_rdd(BaseTestClass.geopysc, SPATIAL, rdd)

        value_map = {0.0: float('nan')}

        result = raster_rdd.reclassify(value_map, float).to_numpy_rdd().first()[1]['data']

        for x in list(result.flatten()):
            self.assertTrue(math.isnan(x))


if __name__ == "__main__":
    unittest.main()
