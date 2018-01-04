import unittest
import pytest
import numpy as np

from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass


class LocalOpertaionsTest(BaseTestClass):
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

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_add_int(self):
        arr = np.zeros((1, 4, 4))

        tile = Tile(arr, 'FLOAT', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = tiled + 1
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == 1).all())

    def test_subtract_double(self):
        arr = np.array([[[1.0, 1.0, 1.0, 1.0],
                         [2.0, 2.0, 2.0, 2.0],
                         [3.0, 3.0, 3.0, 3.0],
                         [4.0, 4.0, 4.0, 4.0]]], dtype=float)

        tile = Tile(arr, 'FLOAT', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = 5.0 - tiled
        actual = result.to_numpy_rdd().first()[1].cells

        expected = np.array([[[4.0, 4.0, 4.0, 4.0],
                              [3.0, 3.0, 3.0, 3.0],
                              [2.0, 2.0, 2.0, 2.0],
                              [1.0, 1.0, 1.0, 1.0]]], dtype=float)

        self.assertTrue((actual == expected).all())

    def test_multiply_double(self):
        arr = np.array([[[1.0, 1.0, 1.0, 1.0],
                         [2.0, 2.0, 2.0, 2.0],
                         [3.0, 3.0, 3.0, 3.0],
                         [4.0, 4.0, 4.0, 4.0]]], dtype=float)

        tile = Tile(arr, 'FLOAT', float('nan'))
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = 5.0 * tiled
        actual = result.to_numpy_rdd().first()[1].cells

        expected = np.array([[[5.0, 5.0, 5.0, 5.0],
                              [10.0, 10.0, 10.0, 10.0],
                              [15.0, 15.0, 15.0, 15.0],
                              [20.0, 20.0, 20.0, 20.0]]], dtype=float)

        self.assertTrue((actual == expected).all())

    def test_divide_tiled_rdd(self):
        arr = np.array([[[5.0, 5.0, 5.0, 5.0],
                         [5.0, 5.0, 5.0, 5.0],
                         [5.0, 5.0, 5.0, 5.0],
                         [5.0, 5.0, 5.0, 5.0]]], dtype=float)

        divider = np.array([[[1.0, 1.0, 1.0, 1.0],
                             [1.0, 1.0, 1.0, 1.0],
                             [1.0, 1.0, 1.0, 1.0],
                             [1.0, 1.0, 1.0, 1.0]]], dtype=float)

        tile = Tile(arr, 'FLOAT', float('nan'))
        tile2 = Tile(divider, 'FLOAT', float('nan'))

        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        rdd2 = BaseTestClass.pysc.parallelize([(self.spatial_key, tile2)])

        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)
        tiled2 = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd2, self.metadata)

        result = tiled / tiled2
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == 5.0).all())

    def test_combined_operations(self):
        arr = np.array([[[10, 10, 10, 10],
                         [20, 20, 20, 20],
                         [10, 10, 10, 10],
                         [20, 20, 20, 20]]], dtype=int)

        tile = Tile(arr, 'INT', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])

        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = (tiled + tiled) / 2
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == arr).all())

    def test_abs_operations(self):
        arr = np.array([[[-10, -10, -10, 10],
                         [-20, -20, -20, 20],
                         [-10, -10, 10, 10],
                         [-20, -20, 20, 20]]], dtype=int)

        expected = np.array([[[10, 10, 10, 10],
                              [20, 20, 20, 20],
                              [10, 10, 10, 10],
                              [20, 20, 20, 20]]], dtype=int)

        tile = Tile(arr, 'INT', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])

        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = abs(tiled)
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == expected).all())

    def test_pow_int(self):
        arr = np.zeros((1, 4, 4))

        tile = Tile(arr, 'FLOAT', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = tiled ** 5
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == 0).all())

    def test_rpow_int(self):
        arr = np.full((1, 4, 4), 2, dtype='int16')

        tile = Tile(arr, 'int16', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = 3 ** tiled
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == 9).all())

    def test_rpow_double(self):
        arr = np.full((1, 4, 4), 3.0, dtype='int64')

        tile = Tile(arr, 'FLOAT', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = 0.0 ** tiled
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == 0.0).all())

    def test_pow_layer(self):
        arr = np.zeros((1, 4, 4))
        twos = arr + 2

        tile = Tile(twos, 'FLOAT', -500)
        rdd = BaseTestClass.pysc.parallelize([(self.spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, self.metadata)

        result = tiled ** tiled
        actual = result.to_numpy_rdd().first()[1].cells

        self.assertTrue((actual == 4).all())


if __name__ == "__main__":
    unittest.main()
