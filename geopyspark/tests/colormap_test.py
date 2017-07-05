import unittest
import pytest
import numpy as np

from geopyspark.geotrellis.color import ColorMap
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass


class ColormapTest(BaseTestClass):
    color_list = [0x000000ff, 0x100000ff, 0x200000ff, 0x300000ff, 0x400000ff, 0x500000ff, 0x600000ff, 0x700000ff]

    def test_from_break_map(self):
        color_list = self.color_list
        break_map = {}
        for i in range(len(color_list)):
            break_map[i] = color_list[i]
        result = ColorMap.from_break_map(BaseTestClass.pysc, break_map)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertTrue(result.cmap.map(3) == color_list[3])

    def test_build1(self):
        color_list = self.color_list
        break_map = {}
        for i in range(len(color_list)):
            break_map[i] = color_list[i]
        result = ColorMap.build(BaseTestClass.pysc, breaks=break_map)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertTrue(result.cmap.map(3) == color_list[3])

    def test_from_colors(self):
        color_list = self.color_list
        breaks = range(len(color_list))
        result = ColorMap.from_colors(BaseTestClass.pysc, breaks, color_list)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertTrue(result.cmap.map(3) == color_list[3])

    def test_build2(self):
        color_list = self.color_list
        breaks = list(range(len(color_list)))
        result = ColorMap.build(BaseTestClass.pysc, breaks=breaks, colors=color_list)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertTrue(result.cmap.map(3) == color_list[3])

    def test_from_histogram(self):
        extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
        layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 2, 'tileRows': 4}
        metadata = {'cellType': 'float32ud-1.0',
                    'extent': extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0},
                        'maxKey': {'col': 0, 'row': 0}},
                    'layoutDefinition': {
                        'extent': extent,
                        'tileLayout': {'tileCols': 2, 'tileRows': 4, 'layoutCols': 1, 'layoutRows': 1}}}
        spatial_key = SpatialKey(0, 0)
        arr = np.array([[[1.0, 5.0, 2.0, 3.0],
                         [4.0, 6.0, 7.0, 0.0]]], dtype=float)
        tile = Tile(arr, 'FLOAT', -500)
        rdd = BaseTestClass.pysc.parallelize([(spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(BaseTestClass.pysc, LayerType.SPATIAL, rdd, metadata)
        hist = tiled.get_histogram()

        color_list = self.color_list
        result = ColorMap.from_histogram(BaseTestClass.pysc, hist, color_list)
        self.assertTrue(isinstance(result, ColorMap))

    def test_build3(self):
        extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
        layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 2, 'tileRows': 4}
        metadata = {'cellType': 'float32ud-1.0',
                    'extent': extent,
                    'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                    'bounds': {
                        'minKey': {'col': 0, 'row': 0},
                        'maxKey': {'col': 0, 'row': 0}},
                    'layoutDefinition': {
                        'extent': extent,
                        'tileLayout': {'tileCols': 2, 'tileRows': 4, 'layoutCols': 1, 'layoutRows': 1}}}
        spatial_key = SpatialKey(0, 0)
        arr = np.array([[[1.0, 5.0, 2.0, 3.0],
                         [4.0, 6.0, 7.0, 0.0]]], dtype=float)
        tile = Tile(arr, 'FLOAT', -500)
        rdd = BaseTestClass.pysc.parallelize([(spatial_key, tile)])
        tiled = TiledRasterLayer.from_numpy_rdd(BaseTestClass.pysc, LayerType.SPATIAL, rdd, metadata)
        hist = tiled.get_histogram()

        color_list = self.color_list
        result = ColorMap.build(BaseTestClass.pysc, breaks=hist, colors=color_list)
        self.assertTrue(isinstance(result, ColorMap))

if __name__ == "__main__":
    unittest.main()
