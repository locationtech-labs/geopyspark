import unittest
import pytest
import numpy as np

from geopyspark.geotrellis import SpatialKey, Extent, Tile, zfactor_lat_lng_calculator
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import LayerType, Unit


class SlopeTest(BaseTestClass):
    cells = np.array([[
        [0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 1.0, 0.0],
        [0.0, 0.0, 1.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 0.0, 0.0]]])

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

    def test_slope(self):

        calc = zfactor_lat_lng_calculator(Unit.METERS)
        result = self.raster_rdd.slope(calc).to_numpy_rdd().values().first().cells

        self.assertEqual(result[0, 0, 0], 0.0)


if __name__ == "__main__":
    unittest.main()
    BaseTestClass.pysc.stop()
