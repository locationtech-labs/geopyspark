import unittest
import datetime
import pytest
import numpy as np

from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.tests.python_test_utils import check_directory, geotiff_test_path
from geopyspark.geotrellis import (GlobalLayout, ProjectedExtent, TemporalProjectedExtent,
                                   Extent, Tile, Bounds, SpatialKey, SpaceTimeKey)
from geopyspark.geotrellis.layer import RasterLayer, TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.union import union
from geopyspark.geotrellis.geotiff import get


check_directory()

epsg_code = 3857
extent = Extent(0.0, 0.0, 10.0, 10.0)
extent_2 = Extent(10.0, 10.0, 20.0, 20.0)

class UnionSpatialTest(BaseTestClass):
    projected_extent_1 = ProjectedExtent(extent, epsg_code)
    projected_extent_2 = ProjectedExtent(extent_2, epsg_code)

    arr = np.zeros((1, 16, 16))
    tile = Tile(arr, 'FLOAT', -500.0)

    rdd_1 = BaseTestClass.pysc.parallelize([(projected_extent_1, tile)])
    rdd_2 = BaseTestClass.pysc.parallelize([(projected_extent_2, tile)])

    layer_1 = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd_1)
    layer_2 = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd_2)

    tiled_layer_1 = layer_1.tile_to_layout(GlobalLayout())
    tiled_layer_2 = layer_2.tile_to_layout(GlobalLayout())

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_union_of_raster_layers(self):
        result = union([self.layer_1, self.layer_2])

        self.assertTrue(result.srdd.rdd().count(), 2)

    def test_union_of_tiled_raster_layers(self):
        result = union([self.tiled_layer_1, self.tiled_layer_2])

        bounds_1 = self.tiled_layer_1.layer_metadata.bounds
        bounds_2 = self.tiled_layer_2.layer_metadata.bounds

        min_col = min(bounds_1.minKey.col, bounds_2.minKey.col)
        min_row = min(bounds_1.minKey.row, bounds_2.minKey.row)
        max_col = max(bounds_1.maxKey.col, bounds_2.maxKey.col)
        max_row = max(bounds_1.maxKey.row, bounds_2.maxKey.row)

        min_key = SpatialKey(min_col, min_row)
        max_key = SpatialKey(max_col, max_row)

        self.assertTrue(result.srdd.rdd().count(), 2)
        self.assertEqual(result.layer_metadata.bounds, Bounds(min_key, max_key))


class UnionTemporalTest(BaseTestClass):
    time_1 = datetime.datetime.strptime("1993-09-19T07:01:00Z", '%Y-%m-%dT%H:%M:%SZ')
    time_2 = datetime.datetime.strptime("2017-09-19T07:01:00Z", '%Y-%m-%dT%H:%M:%SZ')

    temp_projected_extent_1 = TemporalProjectedExtent(extent, time_1, epsg_code)
    temp_projected_extent_2 = TemporalProjectedExtent(extent, time_2, epsg_code)

    arr = np.zeros((1, 16, 16))
    tile = Tile(arr, 'FLOAT', -500.0)

    rdd_1 = BaseTestClass.pysc.parallelize([(temp_projected_extent_1, tile)])
    rdd_2 = BaseTestClass.pysc.parallelize([(temp_projected_extent_2, tile)])

    layer_1 = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd_1)
    layer_2 = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd_2)

    tiled_layer_1 = layer_1.tile_to_layout(GlobalLayout())
    tiled_layer_2 = layer_2.tile_to_layout(GlobalLayout())

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_union_of_raster_layers(self):
        result = union([self.layer_1, self.layer_2])

        self.assertTrue(result.srdd.rdd().count(), 2)

    def test_union_of_tiled_raster_layers(self):
        result = union([self.tiled_layer_1, self.tiled_layer_2])

        bounds_1 = self.tiled_layer_1.layer_metadata.bounds
        bounds_2 = self.tiled_layer_2.layer_metadata.bounds

        min_col = min(bounds_1.minKey.col, bounds_2.minKey.col)
        min_row = min(bounds_1.minKey.row, bounds_2.minKey.row)
        min_instant = min(bounds_1.minKey.instant, bounds_2.minKey.instant)

        max_col = max(bounds_1.maxKey.col, bounds_2.maxKey.col)
        max_row = max(bounds_1.maxKey.row, bounds_2.maxKey.row)
        max_instant = max(bounds_1.maxKey.instant, bounds_2.maxKey.instant)

        min_key = SpaceTimeKey(min_col, min_row, min_instant)
        max_key = SpaceTimeKey(max_col, max_row, max_instant)

        self.assertTrue(result.srdd.rdd().count(), 2)
        self.assertEqual(result.layer_metadata.bounds, Bounds(min_key, max_key))


if __name__ == "__main__":
    unittest.main()
