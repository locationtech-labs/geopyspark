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


class BandSelectionTest(BaseTestClass):
    band_1 = np.array([
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0]])

    band_2 = np.array([
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0],
        [2.0, 2.0, 2.0, 2.0, 2.0]])

    band_3 = np.array([
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0],
        [3.0, 3.0, 3.0, 3.0, 3.0]])

    bands = np.array([band_1, band_2, band_3])

    layer = [(SpatialKey(0, 0), Tile(bands, 'FLOAT', -1.0)),
             (SpatialKey(1, 0), Tile(bands, 'FLOAT', -1.0,)),
             (SpatialKey(0, 1), Tile(bands, 'FLOAT', -1.0,)),
             (SpatialKey(1, 1), Tile(bands, 'FLOAT', -1.0,))]

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

    raster_rdd = TiledRasterLayer.from_numpy_rdd(BaseTestClass.pysc, LayerType.SPATIAL, rdd, metadata)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_bands_int(self):
        actual = self.raster_rdd.bands(1).to_numpy_rdd().first()[1]
        expected = np.array(self.band_2)

        self.assertTrue((expected == actual.cells).all())

    def test_bands_tuple(self):
        actual = self.raster_rdd.bands((1, 2)).to_numpy_rdd().first()[1]
        expected = np.array([self.band_2, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_bands_list(self):
        actual = self.raster_rdd.bands([0, 2]).to_numpy_rdd().first()[1]
        expected = np.array([self.band_1, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_band_range(self):
        actual = self.raster_rdd.bands(range(0, 3)).to_numpy_rdd().first()[1]
        expected = np.array([self.band_1, self.band_2, self.band_3])

        self.assertTrue((expected == actual.cells).all())

if __name__ == "__main__":
    unittest.main()
