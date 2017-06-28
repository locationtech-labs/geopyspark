import unittest
import pytest
import numpy as np

from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass


class HistogramTest(BaseTestClass):
    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
    layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 4, 'tileRows': 4}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0},
                    'maxKey': {'col': 0, 'row': 0}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': {'tileCols': 4, 'tileRows': 4, 'layoutCols': 1, 'layoutRows': 1}}}

    spatial_key = SpatialKey(0, 0)

    arr = np.array([[[1.0, 1.0, 1.0, 1.0],
                     [2.0, 2.0, 2.0, 2.0],
                     [3.0, 3.0, 3.0, 3.0],
                     [4.0, 4.0, 4.0, 4.0]]], dtype=float)

    tile = Tile(arr, 'FLOAT', -500)
    rdd = BaseTestClass.pysc.parallelize([(spatial_key, tile)])
    tiled = TiledRasterLayer.from_numpy_rdd(BaseTestClass.pysc, LayerType.SPATIAL, rdd, metadata)

    hist = tiled.get_histogram()

    def test_min(self):
        self.assertEqual(self.hist.min(), 1.0)

    def test_max(self):
        self.assertEqual(self.hist.max(), 4.0)

    def test_min_max(self):
        self.assertEqual(self.hist.min_max(), (1.0, 4.0))

    def test_mean(self):
        self.assertEqual(self.hist.mean(), 2.5)

    def test_median(self):
        self.assertEqual(self.hist.median(), 2.5)

    def test_cdf(self):
        expected_cdf = [(1.0, 0.25), (2.0, 0.5), (3.0, 0.75), (4.0, 1.0)]

        self.assertEqual(self.hist.cdf(), expected_cdf)

    def test_bucket_count(self):
        self.assertEqual(self.hist.bucket_count(), 4)

    def test_values(self):
        self.assertEqual(self.hist.values(), [1.0, 2.0, 3.0, 4.0])

    def test_merge(self):

        arr2 = np.array([[[1.0, 1.0, 1.0, 1.0],
                          [1.0, 1.0, 1.0, 1.0],
                          [1.0, 1.0, 1.0, 1.0],
                          [1.0, 1.0, 1.0, 1.0]]], dtype=float)

        tile2 = Tile(arr2, 'FLOAT', -500)
        rdd2 = BaseTestClass.pysc.parallelize([(self.spatial_key, tile2)])
        tiled2 = TiledRasterLayer.from_numpy_rdd(BaseTestClass.pysc, LayerType.SPATIAL, rdd2,
                                                 self.metadata)

        hist2 = tiled2.get_histogram()

        merged = self.hist.merge(hist2)


        self.assertEqual(merged.values(), [1.0, 2.0, 3.0, 4.0])
        self.assertEqual(merged.mean(), 1.75)


if __name__ == "__main__":
    unittest.main()
