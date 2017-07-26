import os
import unittest
import numpy as np

import pytest

from shapely.geometry import Point
from geopyspark.geotrellis import SpatialKey, Tile
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import Extent, ProjectedExtent, cost_distance
from geopyspark.geotrellis.layer import RasterLayer, TiledRasterLayer
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

    tiled_raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata, 5)

    layer2 = [(ProjectedExtent(Extent(0, 0, 1, 1), 3857), Tile(bands, 'FLOAT', -1.0)),
              (ProjectedExtent(Extent(1, 0, 2, 1), 3857), Tile(bands, 'FLOAT', -1.0)),
              (ProjectedExtent(Extent(0, 1, 1, 2), 3857), Tile(bands, 'FLOAT', -1.0)),
              (ProjectedExtent(Extent(1, 1, 2, 2), 3857), Tile(bands, 'FLOAT', -1.0))]
    rdd2 = BaseTestClass.pysc.parallelize(layer2)
    raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd2)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_bands_invalid(self):
        with pytest.raises(TypeError):
            self.tiled_raster_rdd.bands("hello").to_numpy_rdd().first()[1]

    def test_bands_int_tiled(self):
        actual = self.tiled_raster_rdd.bands(1).to_numpy_rdd().first()[1]
        expected = np.array(self.band_2)

        self.assertTrue((expected == actual.cells).all())

    def test_bands_int_raster(self):
        actual = self.raster_rdd.bands(1).to_numpy_rdd().first()[1]
        expected = np.array(self.band_2)

        self.assertTrue((expected == actual.cells).all())

    def test_bands_tuple_tiled(self):
        actual = self.tiled_raster_rdd.bands((1, 2)).to_numpy_rdd().first()[1]
        expected = np.array([self.band_2, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_bands_tuple_raster(self):
        actual = self.raster_rdd.bands((1, 2)).to_numpy_rdd().first()[1]
        expected = np.array([self.band_2, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_bands_list_tiled(self):
        actual = self.tiled_raster_rdd.bands([0, 2]).to_numpy_rdd().first()[1]
        expected = np.array([self.band_1, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_bands_list_raster(self):
        actual = self.raster_rdd.bands([0, 2]).to_numpy_rdd().first()[1]
        expected = np.array([self.band_1, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_band_range_tiled(self):
        actual = self.tiled_raster_rdd.bands(range(0, 3)).to_numpy_rdd().first()[1]
        expected = np.array([self.band_1, self.band_2, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_band_range_raster(self):
        actual = self.raster_rdd.bands(range(0, 3)).to_numpy_rdd().first()[1]
        expected = np.array([self.band_1, self.band_2, self.band_3])

        self.assertTrue((expected == actual.cells).all())

    def test_map_tiles_func_tiled(self):
        def test_func(tile):
            cells = tile.cells
            return Tile((cells[0] + cells[1]) / cells[2], tile.cell_type, tile.no_data_value)

        actual = self.tiled_raster_rdd.map_tiles(test_func).to_numpy_rdd().first()[1]
        expected = np.array([self.band_1])

        self.assertTrue((expected == actual.cells).all())

    def test_map_tiles_lambda_tiled(self):
        mapped_layer = self.tiled_raster_rdd.map_tiles(lambda tile: Tile(tile.cells[0], tile.cell_type, tile.no_data_value))
        actual = mapped_layer.to_numpy_rdd().first()[1]
        expected = np.array([self.band_1])

        self.assertEqual(mapped_layer.zoom_level, self.tiled_raster_rdd.zoom_level)
        self.assertTrue((expected == actual.cells).all())

    def test_map_cells_func_raster(self):
        def test_func(cells, nd):
            cells[cells >= 3.0] = nd
            return cells

        actual = self.raster_rdd.map_cells(test_func).to_numpy_rdd().first()[1]

        negative_band = np.array([
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0]])

        expected = np.array([self.band_1, self.band_2, negative_band])

        self.assertTrue((expected == actual.cells).all())

    def test_map_cells_lambda_raster(self):
        actual = self.raster_rdd.map_cells(lambda cells, nd: cells + nd).to_numpy_rdd().first()[1]

        self.assertTrue((0.0 == actual.cells[0, :]).all())
        self.assertTrue((self.band_1 == actual.cells[1, :]).all())
        self.assertTrue((self.band_2 == actual.cells[2, :]).all())

    def test_map_cells_func_tiled(self):
        def test_func(cells, nd):
            cells[cells >= 3.0] = nd
            return cells

        actual = self.tiled_raster_rdd.map_cells(test_func).to_numpy_rdd().first()[1]

        negative_band = np.array([
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0],
            [-1.0, -1.0, -1.0, -1.0, -1.0]])

        expected = np.array([self.band_1, self.band_2, negative_band])

        self.assertTrue((expected == actual.cells).all())

    def test_map_cells_lambda_tiled(self):
        mapped_layer = self.tiled_raster_rdd.map_cells(lambda cells, nd: cells + nd)
        actual = mapped_layer.to_numpy_rdd().first()[1]

        self.assertTrue((0.0 == actual.cells[0, :]).all())
        self.assertTrue((self.band_1 == actual.cells[1, :]).all())
        self.assertTrue((self.band_2 == actual.cells[2, :]).all())
        self.assertEqual(mapped_layer.zoom_level, self.tiled_raster_rdd.zoom_level)

if __name__ == "__main__":
    unittest.main()
