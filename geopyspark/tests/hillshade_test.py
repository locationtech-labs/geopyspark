import os
import unittest
import numpy as np

import pytest

from shapely.geometry import Point
from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import hillshade
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class HillshadeTest(BaseTestClass):
    cells = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 3.0, 3.0, 2.0, 1.0, -1.0],
        [1.0, 1.0, 3.0, 2.0, 2.0, 2.0],
        [1.0, 2.0, 2.0, 2.0, 2.0, 2.0],
        [1.0, 1.0, 1.0, 2.0, 2.0, 2.0],
        [1.0, 1.0, 1.0, 1.0, 1.0, 2.0]]])

    layer = [(SpatialKey(0, 0), Tile(cells, 'FLOAT', -1.0))]

    rdd = BaseTestClass.pysc.parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
    layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 6, 'tileRows': 6}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0},
                    'maxKey': {'col': 0, 'row': 0}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': {'tileCols': 6, 'tileRows': 6, 'layoutCols': 1, 'layoutRows': 1}}}

    raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_hillshade(self):
        result = hillshade(self.raster_rdd, band=0, azimuth=99.0, altitude=33.0, z_factor=0.0)

        data = result.to_numpy_rdd().first()[1].cells[0][0][0]
        self.assertEqual(data, 63)

if __name__ == "__main__":
    unittest.main()
