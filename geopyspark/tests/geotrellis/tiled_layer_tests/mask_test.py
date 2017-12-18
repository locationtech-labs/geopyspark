import os
import unittest
import pytest
import rasterio
import numpy as np

from geopyspark.geotrellis import SpatialKey, Tile
from shapely.geometry import Polygon
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class MaskTest(BaseTestClass):
    pysc = BaseTestClass.pysc

    cells = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0]]])

    layer = [(SpatialKey(0, 0), Tile(cells, 'FLOAT', -1.0)),
             (SpatialKey(1, 0), Tile(cells, 'FLOAT', -1.0,)),
             (SpatialKey(0, 1), Tile(cells, 'FLOAT', -1.0,)),
             (SpatialKey(1, 1), Tile(cells, 'FLOAT', -1.0,))]

    rdd = pysc.parallelize(layer)

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
                    'tileLayout': layout}}

    geometries = Polygon([(17, 17), (42, 17), (42, 42), (17, 42)])
    raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_geotrellis_mask(self):
        result = self.raster_rdd.mask(geometries=self.geometries).to_numpy_rdd()
        n = result.map(lambda kv: np.sum(kv[1].cells)).reduce(lambda a,b: a + b)
        self.assertEqual(n, 25.0)

if __name__ == "__main__":
    unittest.main()
