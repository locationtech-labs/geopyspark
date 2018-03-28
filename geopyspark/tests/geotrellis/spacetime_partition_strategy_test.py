import os
import datetime
import unittest
import numpy as np

import pytest

from geopyspark.geotrellis import SpatialKey, Tile, _convert_to_unix_time
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import Extent, ProjectedExtent, SpaceTimeKey, SpatialKey, TemporalProjectedExtent, SpaceTimePartitionStrategy
from geopyspark.geotrellis.layer import RasterLayer, TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType, TimeUnit


class SpaceTimePartitionStrategyTest(BaseTestClass):
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

    strategy = SpaceTimePartitionStrategy(TimeUnit.MONTHS, num_partitions=8)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_tiled_raster_layer_should_partition_by(self):
        result = self.tiled_raster_rdd.partitionBy(self.strategy)
        actual_strategy = result.get_partition_strategy()

        self.assertEqual(self.strategy, actual_strategy)

    def test_raster_layer_tile_to_layout(self):
        result = self.raster_rdd.tile_to_layout(layout=self.tiled_raster_rdd, partition_strategy=self.strategy)
        actual_strategy = result.get_partition_strategy()

        self.assertEqual(self.strategy, actual_strategy)



if __name__ == "__main__":
    unittest.main()
