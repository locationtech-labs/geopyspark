import os
import unittest
import numpy as np
import pytest

from geopyspark.geotrellis import SpatialKey, Tile
from shapely.geometry import Point
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import LayerType


class StitchTest(BaseTestClass):
    cells = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 0.0]]])

    layer = [(SpatialKey(0, 0), Tile(np.array([cells, cells]), 'FLOAT', -1.0)),
             (SpatialKey(1, 0), Tile(np.array([cells, cells]), 'FLOAT', -1.0,)),
             (SpatialKey(0, 1), Tile(np.array([cells, cells]), 'FLOAT', -1.0,)),
             (SpatialKey(1, 1), Tile(np.array([cells, cells]), 'FLOAT', -1.0,))]
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

    @pytest.fixture(scope='class', autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_stitch(self):
        result = self.raster_rdd.stitch()
        self.assertTrue(result.cells.shape == (2, 10, 10))


if __name__ == "__main__":
    unittest.main()
