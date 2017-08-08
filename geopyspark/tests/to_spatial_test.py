import os
import datetime
import unittest
import numpy as np

import pytest

from geopyspark.geotrellis import SpatialKey, Tile, _convert_to_unix_time
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import Extent, ProjectedExtent, SpaceTimeKey, SpatialKey, TemporalProjectedExtent
from geopyspark.geotrellis.layer import RasterLayer, TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class ToSpatialLayerTest(BaseTestClass):
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

    tile_1 = Tile.from_numpy_array(np.array([band_1]))
    tile_2 = Tile.from_numpy_array(np.array([band_2]))
    time_1 = datetime.datetime.strptime("2016-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_2 = datetime.datetime.strptime("2017-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')

    layer = [(SpaceTimeKey(0, 0, time_1), tile_1),
             (SpaceTimeKey(1, 0, time_1), tile_1),
             (SpaceTimeKey(0, 1, time_1), tile_1),
             (SpaceTimeKey(1, 1, time_1), tile_1),
             (SpaceTimeKey(0, 0, time_2), tile_2),
             (SpaceTimeKey(1, 0, time_2), tile_2),
             (SpaceTimeKey(0, 1, time_2), tile_2),
             (SpaceTimeKey(1, 1, time_2), tile_2)
            ]

    rdd = BaseTestClass.pysc.parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 5, 'tileRows': 5}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(time_1)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(time_2)}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 2, 'layoutRows': 2}}}

    tiled_raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    layer2 = [(TemporalProjectedExtent(Extent(0, 0, 1, 1), epsg=3857, instant=time_1), tile_1),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_1), tile_1),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_1), tile_1),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_1), tile_1),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_2), tile_2),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_2), tile_2),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_2), tile_2),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_2), tile_2)]

    rdd2 = BaseTestClass.pysc.parallelize(layer2)
    raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd2)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    # This test should be moved to a more appropriate file once more spatial-temporal
    # tests are made.
    def test_spatial_metadata(self):
        metadata = self.raster_rdd.collect_metadata()
        min_key = metadata.bounds.minKey
        max_key = metadata.bounds.maxKey

        self.assertEqual(min_key.instant, self.time_1)
        self.assertEqual(max_key.instant, self.time_2)

    def test_to_spatial_raster_layer(self):
        actual = self.raster_rdd.to_spatial_layer().to_numpy_rdd().keys().collect()

        expected = [
            ProjectedExtent(Extent(0, 0, 1, 1), 3857),
            ProjectedExtent(Extent(1, 0, 2, 1), 3857),
            ProjectedExtent(Extent(0, 1, 1, 2), 3857),
            ProjectedExtent(Extent(1, 1, 2, 2), 3857)
        ]

        for x in actual:
            self.assertTrue(x in expected)

    def test_to_spatial_target_time_raster_layer(self):
        converted = self.raster_rdd.to_spatial_layer(target_time=self.time_1)
        keys = converted.to_numpy_rdd().keys().collect()
        values = converted.to_numpy_rdd().values().collect()

        expected = [
            ProjectedExtent(Extent(0, 0, 1, 1), 3857),
            ProjectedExtent(Extent(1, 0, 2, 1), 3857),
            ProjectedExtent(Extent(0, 1, 1, 2), 3857),
            ProjectedExtent(Extent(1, 1, 2, 2), 3857)
        ]

        for x in keys:
            self.assertTrue(x in expected)

        for x in values:
            self.assertEqual(x.cells.shape, self.tile_1.cells.shape)
            self.assertTrue((x.cells == 1.0).all())

    def test_to_spatial_tiled_layer(self):
        actual = self.tiled_raster_rdd.to_spatial_layer().to_numpy_rdd().keys().collect()

        expected = [
            SpatialKey(0, 0),
            SpatialKey(1, 0),
            SpatialKey(0, 1),
            SpatialKey(1, 1)
        ]

        for x in actual:
            self.assertTrue(x in expected)

    def test_to_spatial_target_time_tiled_layer(self):
        converted = self.tiled_raster_rdd.to_spatial_layer(target_time=self.time_2)
        keys = converted.to_numpy_rdd().keys().collect()
        values = converted.to_numpy_rdd().values().collect()

        expected = [
            SpatialKey(0, 0),
            SpatialKey(1, 0),
            SpatialKey(0, 1),
            SpatialKey(1, 1)
        ]

        for x in keys:
            self.assertTrue(x in expected)

        for x in values:
            self.assertEqual(x.cells.shape, self.tile_2.cells.shape)
            self.assertTrue((x.cells == 2.0).all())


if __name__ == "__main__":
    unittest.main()
