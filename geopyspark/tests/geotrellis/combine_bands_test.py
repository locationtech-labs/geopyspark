import os
import datetime
import unittest
import pytest
import numpy as np

from geopyspark.geotrellis import (SpatialKey, SpaceTimeKey, Extent,
                                   Tile, ProjectedExtent, TemporalProjectedExtent,
                                   LocalLayout)
from geopyspark.geotrellis.combine_bands import combine_bands
from geopyspark.geotrellis.layer import TiledRasterLayer, RasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import LayerType, Operation


now = datetime.datetime.now()
crs = '+proj=longlat +datum=WGS84 +no_defs '
extent = Extent(0.0, 0.0, 33.0, 33.0)

e_1 = Extent(0, 0, 16.5, 16.5)
e_2 = Extent(0, 16.5, 16.5, 33)
e_3 = Extent(16.5, 16.5, 33, 33)
e_4 = Extent(16.5, 0, 33, 16.5)

def create_spatial_layer(tile):
    layer = [(ProjectedExtent(e_1, proj4=crs), tile),
             (ProjectedExtent(e_2, proj4=crs), tile),
             (ProjectedExtent(e_3, proj4=crs), tile),
             (ProjectedExtent(e_4, proj4=crs), tile)]

    return layer

def create_temporal_layer(tile):
    layer = [(TemporalProjectedExtent(e_1, now, proj4=crs), tile),
             (TemporalProjectedExtent(e_2, now, proj4=crs), tile),
             (TemporalProjectedExtent(e_3, now, proj4=crs), tile),
             (TemporalProjectedExtent(e_4, now, proj4=crs), tile)]

    return layer

first = np.array([[
    [1.0, 2.0, 3.0, 4.0, 5.0],
    [1.0, 2.0, 3.0, 4.0, 5.0],
    [1.0, 2.0, 3.0, 4.0, 5.0],
    [1.0, 2.0, 3.0, 4.0, 5.0],
    [1.0, 2.0, 3.0, 4.0, 5.0]]])

second = np.array([[
    [0.0, 0.0, 0.0, 0.0, 0.0],
    [0.0, 0.0, 0.0, 0.0, 0.0],
    [0.0, 0.0, 0.0, 0.0, 0.0],
    [0.0, 0.0, 0.0, 0.0, 0.0],
    [0.0, 0.0, 0.0, 0.0, 0.0]]])

third = np.array([[
    [10.0, 10.0, 10.0, 10.0, 10.0],
    [10.0, 10.0, 10.0, 10.0, 10.0],
    [10.0, 10.0, 10.0, 10.0, 10.0],
    [10.0, 10.0, 10.0, 10.0, 10.0],
    [10.0, 10.0, 10.0, 10.0, 10.0]]])

tile_1 = Tile.from_numpy_array(first, -1.0)
tile_2 = Tile.from_numpy_array(second, -1.0)
tile_3 = Tile.from_numpy_array(third, -1.0)


class CombineSpatialBandsTest(BaseTestClass):
    layer_1 = create_spatial_layer(tile_1)
    layer_2 = create_spatial_layer(tile_2)
    layer_3 = create_spatial_layer(tile_3)

    r1 = BaseTestClass.pysc.parallelize(layer_1)
    r2 = BaseTestClass.pysc.parallelize(layer_2)
    r3 = BaseTestClass.pysc.parallelize(layer_3)

    rdd_1 = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, r1)
    rdd_2 = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, r2)
    rdd_3 = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, r3)

    tiled_rdd_1 = rdd_1.tile_to_layout(LocalLayout(5, 5))
    tiled_rdd_2 = rdd_2.tile_to_layout(LocalLayout(5, 5))
    tiled_rdd_3 = rdd_3.tile_to_layout(LocalLayout(5, 5))

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_combine_bands_raster_layers(self):
        actual = combine_bands([self.rdd_1, self.rdd_2, self.rdd_3]).to_numpy_rdd().values().collect()

        for x in actual:
            self.assertEqual(x.cells.shape, (3, 5, 5))
            self.assertTrue((x.cells[0, :, :] == first).all())
            self.assertTrue((x.cells[1, :, :] == second).all())
            self.assertTrue((x.cells[2, :, :] == third).all())

    def test_combine_bands_tiled_layers(self):
        actual = combine_bands([self.tiled_rdd_3, self.tiled_rdd_2, self.tiled_rdd_1]) \
                .to_numpy_rdd().values().collect()

        for x in actual:
            self.assertEqual(x.cells.shape, (3, 5, 5))
            self.assertTrue((x.cells[0, :, :] == third).all())
            self.assertTrue((x.cells[1, :, :] == second).all())
            self.assertTrue((x.cells[2, :, :] == first).all())


class CombineTemporalBandsTest(BaseTestClass):
    layer_1 = create_temporal_layer(tile_1)
    layer_2 = create_temporal_layer(tile_2)
    layer_3 = create_temporal_layer(tile_3)

    r1 = BaseTestClass.pysc.parallelize(layer_1)
    r2 = BaseTestClass.pysc.parallelize(layer_2)
    r3 = BaseTestClass.pysc.parallelize(layer_3)

    rdd_1 = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, r1)
    rdd_2 = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, r2)
    rdd_3 = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, r3)

    tiled_rdd_1 = rdd_1.tile_to_layout(LocalLayout(5, 5))
    tiled_rdd_2 = rdd_2.tile_to_layout(LocalLayout(5, 5))
    tiled_rdd_3 = rdd_3.tile_to_layout(LocalLayout(5, 5))

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_combine_bands_raster_layers(self):
        actual = combine_bands([self.rdd_3, self.rdd_2]).to_numpy_rdd().values().collect()

        for x in actual:
            self.assertEqual(x.cells.shape, (2, 5, 5))
            self.assertTrue((x.cells[0, :, :] == third).all())
            self.assertTrue((x.cells[1, :, :] == second).all())

    def test_combine_bands_tiled_layers(self):
        actual = combine_bands([self.tiled_rdd_2, self.tiled_rdd_1, self.tiled_rdd_3]) \
                .to_numpy_rdd().values().collect()

        for x in actual:
            self.assertEqual(x.cells.shape, (3, 5, 5))
            self.assertTrue((x.cells[0, :, :] == second).all())
            self.assertTrue((x.cells[1, :, :] == first).all())
            self.assertTrue((x.cells[2, :, :] == third).all())


if __name__ == "__main__":
    unittest.main()
    BaseTestClass.pysc.stop()
