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


class FilterByTimesTest(BaseTestClass):
    band = np.array([
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0]])

    tile = Tile.from_numpy_array(band)
    time_1 = datetime.datetime.strptime("2016-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_2 = datetime.datetime.strptime("2017-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_3 = datetime.datetime.strptime("2017-10-17T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')

    layer = [(SpaceTimeKey(0, 0, time_1), tile),
             (SpaceTimeKey(1, 0, time_1), tile),
             (SpaceTimeKey(0, 1, time_1), tile),
             (SpaceTimeKey(1, 1, time_1), tile),
             (SpaceTimeKey(0, 0, time_2), tile),
             (SpaceTimeKey(1, 0, time_2), tile),
             (SpaceTimeKey(0, 1, time_2), tile),
             (SpaceTimeKey(1, 1, time_2), tile),
             (SpaceTimeKey(0, 0, time_3), tile),
             (SpaceTimeKey(1, 0, time_3), tile),
             (SpaceTimeKey(0, 1, time_3), tile),
             (SpaceTimeKey(1, 1, time_3), tile)
            ]

    rdd = BaseTestClass.pysc.parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 5, 'tileRows': 5}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0, 'instant': _convert_to_unix_time(time_1)},
                    'maxKey': {'col': 1, 'row': 1, 'instant': _convert_to_unix_time(time_3)}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 2, 'layoutRows': 2}}}

    tiled_raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, metadata)

    layer2 = [(TemporalProjectedExtent(Extent(0, 0, 1, 1), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_1), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_2), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_3), tile),
              (TemporalProjectedExtent(Extent(1, 0, 2, 1), epsg=3857, instant=time_3), tile),
              (TemporalProjectedExtent(Extent(0, 1, 1, 2), epsg=3857, instant=time_3), tile),
              (TemporalProjectedExtent(Extent(1, 1, 2, 2), epsg=3857, instant=time_3), tile)]

    rdd2 = BaseTestClass.pysc.parallelize(layer2)
    raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd2)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_filter_temporal_projected_extent_single_time(self):
        result = self.raster_rdd.filter_by_times([self.time_1])
        expected = self.layer2[:4]
        actual = result.to_numpy_rdd().collect()

        self.assertEqual(len(expected), len(actual))

        for x, y in zip(expected, actual):
            self.assertEqual(x[0], y[0])
            self.assertTrue((x[1].cells == y[1].cells).all())

    def test_filter_temporal_projected_extent_multi_intervals(self):
        result = self.raster_rdd.filter_by_times([self.time_2, self.time_3])
        expected = self.layer2[4:]
        actual = result.to_numpy_rdd().collect()

        self.assertEqual(len(expected), len(actual))

        for x, y in zip(expected, actual):
            self.assertEqual(x[0], y[0])
            self.assertTrue((x[1].cells == y[1].cells).all())

    def test_filter_spacetime_key_single_time(self):
        result = self.tiled_raster_rdd.filter_by_times([self.time_3])
        expected = self.layer[8:]
        actual = result.to_numpy_rdd().collect()

        self.assertEqual(len(expected), len(actual))

        for x, y in zip(expected, actual):
            self.assertEqual(x[0], y[0])
            self.assertTrue((x[1].cells == y[1].cells).all())

    def test_filter_spacetime_key_multi_intervals(self):
        result = self.tiled_raster_rdd.filter_by_times([self.time_1, self.time_2])
        expected = self.layer[:8]
        actual = result.to_numpy_rdd().collect()

        self.assertEqual(len(expected), len(actual))

        for x, y in zip(expected, actual):
            self.assertEqual(x[0], y[0])
            self.assertTrue((x[1].cells == y[1].cells).all())


if __name__ == "__main__":
    unittest.main()
