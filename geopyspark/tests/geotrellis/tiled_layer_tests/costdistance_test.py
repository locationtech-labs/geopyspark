import os
import unittest
import numpy as np

import pytest

from shapely.geometry import Point
from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import cost_distance
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class CostDistanceTest(BaseTestClass):
    cells = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 0.0]]])

    tile = Tile.from_numpy_array(cells, -1.0)

    layer = [(SpatialKey(0, 0), tile),
             (SpatialKey(1, 0), tile),
             (SpatialKey(0, 1), tile),
             (SpatialKey(1, 1), tile)]

    rdd = BaseTestClass.pysc.parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 5, 'tileRows': 5}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0},
                    'maxKey': {'col': 1, 'row': 1}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 2, 'layoutRows': 2}}}

    raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_costdistance_finite(self):
        def zero_one(kv):
            k = kv[0]
            return (k.col == 0 and k.row == 1)

        result = cost_distance(self.raster_rdd,
                               geometries=[Point(13, 13)], max_distance=144000.0)

        tile = result.to_numpy_rdd().filter(zero_one).first()[1]
        point_distance = tile.cells[0][1][3]
        self.assertEqual(point_distance, 0.0)

    def test_costdistance_finite_int(self):
        def zero_one(kv):
            k = kv[0]
            return (k.col == 0 and k.row == 1)

        result = cost_distance(self.raster_rdd,
                               geometries=[Point(13, 13)], max_distance=144000)

        tile = result.to_numpy_rdd().filter(zero_one).first()[1]
        point_distance = tile.cells[0][1][3]
        self.assertEqual(point_distance, 0.0)

    def test_costdistance_infinite(self):
        def zero_one(kv):
            k = kv[0]
            return (k.col == 0 and k.row == 1)

        result = cost_distance(self.raster_rdd,
                               geometries=[Point(13, 13)], max_distance=float('inf'))

        tile = result.to_numpy_rdd().filter(zero_one).first()[1]
        point_distance = tile.cells[0][0][0]
        self.assertTrue(point_distance > 1250000)

if __name__ == "__main__":
    unittest.main()
