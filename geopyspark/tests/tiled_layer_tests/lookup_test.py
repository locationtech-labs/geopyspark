import os
import unittest
import numpy as np

import pytest

from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class LookupTest(BaseTestClass):
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

    def test_lookup_1(self):
        result = self.raster_rdd.lookup(0, 0)[0]
        n = np.sum(result.cells)
        self.assertEqual(n, 24 + 0*25)

    def test_lookup_2(self):
        result = self.raster_rdd.lookup(0, 1)[0]
        n = np.sum(result.cells)
        self.assertEqual(n, 24 + 2*25)

    def test_lookup_3(self):
        result = self.raster_rdd.lookup(1, 0)[0]
        n = np.sum(result.cells)
        self.assertEqual(n, 24 + 1*25)

    def test_lookup_4(self):
        result = self.raster_rdd.lookup(1, 1)[0]
        n = np.sum(result.cells)
        self.assertEqual(n, 24 + 3*25)

    def test_invalid_1(self):
        with pytest.raises(IndexError):
            result = self.raster_rdd.lookup(13, 33)

    def test_invalid_2(self):
        with pytest.raises(IndexError):
            result = self.raster_rdd.lookup(33, 13)



if __name__ == "__main__":
    unittest.main()
