import os
import unittest
import pytest
import rasterio
import numpy as np

from geopyspark.geotrellis import SpatialKey, Tile
from shapely.geometry import box
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class CellValueCountsTest(BaseTestClass):
    pysc = BaseTestClass.pysc

    cells = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [4.0, 4.0, 4.0, 4.0, 4.0],
        [5.0, 5.0, 5.0, 5.0, 5.0]]])

    layer = [(SpatialKey(0, 0), Tile(cells, 'FLOAT', -1.0)),
             (SpatialKey(1, 0), Tile(cells, 'FLOAT', -1.0,)),
             (SpatialKey(0, 1), Tile(cells, 'FLOAT', -1.0,)),
             (SpatialKey(1, 1), Tile(cells, 'FLOAT', -1.0,))]

    rdd = pysc.parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 4.0, 'ymax': 4.0}
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

    raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_counts_with_no_area(self):
        actual = self.raster_rdd.get_cell_value_counts()
        expected = {
            1: 20,
            2: 20,
            3: 20,
            4: 20,
            5: 20
        }

        self.assertDictEqual(actual, expected)

    def test_counts_with_polygon(self):
        area_of_interest = box(0.0, 0.0, 2.0, 2.0)
        actual = self.raster_rdd.get_cell_value_counts(area_of_interest)
        expected = {
            1: 5,
            2: 5,
            3: 5,
            4: 5,
            5: 5
        }

        self.assertDictEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
