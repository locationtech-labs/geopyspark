import os
import unittest
import numpy as np

import pytest

from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class NormalizeTest(BaseTestClass):
    cells = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 0.0]]])

    layer = [(SpatialKey(0, 0), Tile(cells + 0, 'FLOAT', -1.0)),
             (SpatialKey(1, 0), Tile(cells + 1, 'FLOAT', -1.0,)),
             (SpatialKey(0, 1), Tile(cells + 2, 'FLOAT', -1.0,)),
             (SpatialKey(1, 1), Tile(cells + 3, 'FLOAT', -1.0,))]

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

    def test_normalize_all_parameters(self):
        normalized = self.raster_rdd.normalize(old_min=0.0, old_max=4.0, new_min=5.0, new_max=10.0)

        self.assertEqual(normalized.get_min_max(), (5.0, 10.0))

    def test_normalize_no_optinal_parameters(self):
        normalized = self.raster_rdd.normalize(new_min=5.0, new_max=10.0)

        self.assertEqual(normalized.get_min_max(), (5.0, 10.0))

    def test_normalize_old_min(self):
        normalized = self.raster_rdd.normalize(old_min=-1, new_min=5.0, new_max=10.0)

        self.assertEqual(normalized.get_min_max(), (6.0, 10.0))

    def test_normalize_old_max(self):
        normalized = self.raster_rdd.normalize(old_max=5.0, new_min=5.0, new_max=10.0)

        self.assertEqual(normalized.get_min_max(), (5.0, 9.0))


if __name__ == "__main__":
    unittest.main()
