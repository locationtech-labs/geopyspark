import numpy as np
import pytest
import struct
import sys
import unittest

from colortools import Color
from geopyspark.geotrellis.color import get_colors_from_colors, get_colors_from_matplotlib, ColorMap
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass


class ColormapTest(BaseTestClass):
    color_list = [0x000000ff, 0x100000ff, 0x200000ff, 0x300000ff, 0x400000ff, 0x500000ff, 0x600000ff, 0x700000ff]

    def test_get_colors_from_colors(self):
        colors = [Color('red'), Color('green'), Color('blue')]
        result = get_colors_from_colors(colors)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], 0xff0000ff)

    @pytest.mark.skipif(sys.version_info < (3,4),
                        reason="Python 3.4 or greater needed for matplotlib")
    def test_get_colors_from_matplotlib(self):
        result = get_colors_from_matplotlib('viridis', num_colors=42)
        self.assertEqual(len(result), 42)
        self.assertEqual(result[0], 0x440154ff)

    def test_nlcd(self):
        result = ColorMap.nlcd_colormap()
        self.assertEqual(result.cmap.map(0), 0x00000000)
        self.assertEqual((0x0ffffffff + 1 + result.cmap.map(51)), 0xbaa65cff)
        self.assertEqual((0x0ffffffff + 1 + result.cmap.map(92)), 0xb6d8f5ff)

    def test_from_break_map_int(self):
        color_list = self.color_list
        break_map = {}
        for i in range(len(color_list)):
            break_map[i] = color_list[i]
        result = ColorMap.from_break_map(break_map)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.map(3), color_list[3])

    def test_from_break_map_float(self):
        color_list = self.color_list
        break_map = {}
        for i in range(len(color_list)):
            break_map[float(i)] = color_list[i]
        result = ColorMap.from_break_map(break_map)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.map(3), color_list[3])

    def test_from_break_map_str(self):
        color_list = self.color_list
        break_map = {}
        for i in range(len(color_list)):
            break_map[str(i)] = color_list[i]
        with pytest.raises(TypeError):
            result = ColorMap.from_break_map(break_map)

    def test_from_colors_int(self):
        color_list = self.color_list
        breaks = range(len(color_list))
        result = ColorMap.from_colors(breaks, color_list)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.map(3), color_list[3])

    def test_from_colors_float(self):
        color_list = self.color_list
        breaks = map(float, range(len(color_list)))
        result = ColorMap.from_colors(breaks, color_list)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.mapDouble(3.0), color_list[2]) # XXX

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
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)
        hist = tiled.get_histogram()

        color_list = self.color_list
        result = ColorMap.from_histogram(hist, color_list)
        self.assertTrue(isinstance(result, ColorMap))

    def test_build_map(self):
        color_list = self.color_list
        break_map = {}
        for i in range(len(color_list)):
            break_map[i] = color_list[i]
        result = ColorMap.build(breaks=break_map)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.map(3), color_list[3])

    def test_build_int_list(self):
        color_list = self.color_list
        breaks = list(range(len(color_list)))
        result = ColorMap.build(breaks=breaks, colors=color_list)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.map(3), color_list[3])

    def test_build_color_list(self):
        color_list = [Color('red'), Color('green'), Color('blue')] # XXX
        breaks = list(range(len(color_list)))
        result = ColorMap.build(breaks=breaks, colors=color_list)
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.map(2), struct.unpack(">L", bytes(color_list[2].rgba))[0])

    @pytest.mark.skipif(sys.version_info < (3,4),
                        reason="Python 3.4 or greater needed for matplotlib")
    def test_build_color_string(self):
        breaks = list(range(42))
        result = ColorMap.build(breaks=breaks, colors='viridis')
        self.assertTrue(isinstance(result, ColorMap))
        self.assertEqual(result.cmap.map(0), 0x440154ff)

    def test_build_histogram(self):
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
        tiled = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)
        hist = tiled.get_histogram()

        color_list = self.color_list
        result = ColorMap.build(breaks=hist, colors=color_list)
        self.assertTrue(isinstance(result, ColorMap))

if __name__ == "__main__":
    unittest.main()
