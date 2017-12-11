import numpy as np
import os
import pytest
import unittest

from geopyspark.geotrellis import SpatialKey, Extent, Tile
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import LayerType, Operation


class AggregateCellsTest(BaseTestClass):
    first = np.array([[
        [1.0, 2.0, 3.0, 4.0, 5.0],
        [1.0, 2.0, 3.0, 4.0, 5.0],
        [1.0, 2.0, 3.0, 4.0, 5.0],
        [1.0, 2.0, 3.0, 4.0, 5.0],
        [1.0, 2.0, 3.0, 4.0, 5.0]]])

    second = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [4.0, 4.0, 4.0, 4.0, 4.0],
        [5.0, 5.0, 5.0, 5.0, 5.0]]])

    cells_1 = np.array([first, second])
    cells_2 = np.array([second, first])

    tile_1 = Tile.from_numpy_array(cells_1, -1.0)
    tile_2 = Tile.from_numpy_array(cells_2, -1.0)

    layer = [(SpatialKey(0, 0), tile_1),
             (SpatialKey(1, 0), tile_1),
             (SpatialKey(1, 0), tile_2),
             (SpatialKey(0, 1), tile_1),
             (SpatialKey(1, 1), tile_1)]
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

    def test_aggregate_sum(self):
        result = self.raster_rdd.aggregate_by_cell(operation=Operation.SUM)
        expected = np.array([self.first + self.second, self.first + self.second])

        self.assertTrue((result.lookup(1, 0)[0].cells == expected).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[0] == self.first).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[1] == self.second).all())

    def test_aggregate_min(self):
        result = self.raster_rdd.aggregate_by_cell(operation=Operation.MIN)

        band = np.array([[
            [1, 1, 1, 1, 1],
            [1, 2, 2, 2, 2],
            [1, 2, 3, 3, 3],
            [1, 2, 3, 4, 4],
            [1, 2, 3, 4, 5]]])

        expected = np.array([band, band])

        self.assertTrue((result.lookup(1, 0)[0].cells == expected).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[0] == self.first).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[1] == self.second).all())

    def test_aggregate_max(self):
        result = self.raster_rdd.aggregate_by_cell(operation=Operation.MAX)

        band = np.array([[
            [1, 2, 3, 4, 5],
            [2, 2, 3, 4, 5],
            [3, 3, 3, 4, 5],
            [4, 4, 4, 4, 5],
            [5, 5, 5, 5, 5]]])

        expected = np.array([band, band])

        self.assertTrue((result.lookup(1, 0)[0].cells == expected).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[0] == self.first).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[1] == self.second).all())

    def test_aggregate_mean(self):
        result = self.raster_rdd.aggregate_by_cell(Operation.MEAN)

        band = np.array([[
            [1,   1.5, 2,   2.5, 3],
            [1.5, 2,   2.5, 3,   3.5],
            [2,   2.5, 3,   3.5, 4],
            [2.5, 3,   3.5, 4,   4.5],
            [3,   3.5, 4,   4.5, 5]]])

        expected = np.array([band, band])

        self.assertTrue((result.lookup(1, 0)[0].cells == expected).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[0] == self.first).all())
        self.assertTrue((result.lookup(0, 0)[0].cells[1] == self.second).all())

    def test_aggregate_variance(self):
        result = self.raster_rdd.aggregate_by_cell(Operation.VARIANCE)

        band = np.array([[
            [1,   1.5, 2,   2.5, 3],
            [1.5, 2,   2.5, 3,   3.5],
            [2,   2.5, 3,   3.5, 4],
            [2.5, 3,   3.5, 4,   4.5],
            [3,   3.5, 4,   4.5, 5]]])

        expected = np.array([
            ((self.first - band) ** 2) + ((self.second - band) ** 2),
            ((self.first - band) ** 2) + ((self.second - band) ** 2)
        ])
        expected_2 = np.full((5, 5), -1.0)

        self.assertTrue((result.lookup(1, 0)[0].cells == expected).all())
        self.assertTrue((result.lookup(0, 0)[0].cells == expected_2).all())

    def test_aggregate_std(self):
        result = self.raster_rdd.aggregate_by_cell(Operation.STANDARD_DEVIATION)

        band = np.array([[
            [1,   1.5, 2,   2.5, 3],
            [1.5, 2,   2.5, 3,   3.5],
            [2,   2.5, 3,   3.5, 4],
            [2.5, 3,   3.5, 4,   4.5],
            [3,   3.5, 4,   4.5, 5]]])

        expected = np.array([
            (((self.first - band) ** 2) + ((self.second - band) ** 2)) ** (1/2),
            (((self.first - band) ** 2) + ((self.second - band) ** 2)) ** (1/2)
        ])
        expected_2 = np.full((5, 5), -1.0)

        self.assertTrue((result.lookup(1, 0)[0].cells == expected).all())
        self.assertTrue((result.lookup(0, 0)[0].cells == expected_2).all())


if __name__ == "__main__":
    unittest.main()
    BaseTestClass.pysc.stop()
