import datetime
import unittest
import pytest
import numpy as np

from geopyspark.geotrellis import (ProjectedExtent,
                                   Extent,
                                   TemporalProjectedExtent,
                                   SpatialKey,
                                   SpaceTimeKey,
                                   Tile,
                                   Bounds,
                                   TileLayout,
                                   LayoutDefinition,
                                   Metadata)
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import RasterLayer, TiledRasterLayer
from geopyspark.geotrellis.constants import LayerType


class MergeTest(BaseTestClass):
    arr_1 = np.zeros((1, 4, 4))
    arr_2 = np.ones((1, 4, 4))

    tile_1 = Tile.from_numpy_array(arr_1)
    tile_2 = Tile.from_numpy_array(arr_2)

    crs = 4326
    time = datetime.datetime.strptime("2016-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')

    extents = [
        Extent(0.0, 0.0, 4.0, 4.0),
        Extent(0.0, 4.0, 4.0, 8.0),
    ]

    extent = Extent(0.0, 0.0, 8.0, 8.0)
    layout = TileLayout(2, 2, 5, 5)

    ct = 'float32ud-1.0'
    md_proj = '+proj=longlat +datum=WGS84 +no_defs '
    ld = LayoutDefinition(extent, layout)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_projected_extent(self):
        pes = [
            ProjectedExtent(extent=self.extents[0], epsg=self.crs),
            ProjectedExtent(extent=self.extents[1], epsg=self.crs),
        ]

        pe_layer = [
            (pes[0], self.tile_1),
            (pes[0], self.tile_2),
            (pes[1], self.tile_1),
            (pes[1], self.tile_2)
        ]

        rdd = self.pysc.parallelize(pe_layer)
        layer = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)

        actual = layer.merge()

        self.assertEqual(actual.srdd.rdd().count(), 2)

        for k, v in actual.to_numpy_rdd().collect():
            self.assertTrue((v.cells == self.arr_2).all())

    def test_temporal_projected_extent(self):
        pes = [
            TemporalProjectedExtent(extent=self.extents[0], epsg=self.crs, instant=self.time),
            TemporalProjectedExtent(extent=self.extents[1], epsg=self.crs, instant=self.time),
        ]

        pe_layer = [
            (pes[0], self.tile_1),
            (pes[1], self.tile_1),
            (pes[0], self.tile_2),
            (pes[1], self.tile_2)
        ]

        rdd = self.pysc.parallelize(pe_layer)
        layer = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd)

        actual = layer.merge()

        self.assertEqual(actual.srdd.rdd().count(), 2)

        for k, v in actual.to_numpy_rdd().collect():
            self.assertTrue((v.cells == self.arr_2).all())

    def test_spatial_keys(self):
        keys = [
            SpatialKey(0, 0),
            SpatialKey(0, 1)
        ]

        key_layer = [
            (keys[0], self.tile_1),
            (keys[1], self.tile_1),
            (keys[0], self.tile_2),
            (keys[1], self.tile_2)
        ]

        bounds = Bounds(keys[0], keys[1])

        md = Metadata(bounds=bounds,
                      crs=self.md_proj,
                      cell_type=self.ct,
                      extent=self.extent,
                      layout_definition=self.ld)

        rdd = self.pysc.parallelize(key_layer)
        layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, md)

        actual = layer.merge()

        self.assertEqual(actual.srdd.rdd().count(), 2)

        for k, v in actual.to_numpy_rdd().collect():
            self.assertTrue((v.cells == self.arr_2).all())

    def test_space_time_keys(self):
        temp_keys = [
            SpaceTimeKey(0, 0, instant=self.time),
            SpaceTimeKey(0, 1, instant=self.time)
        ]

        temp_key_layer = [
            (temp_keys[0], self.tile_2),
            (temp_keys[1], self.tile_2),
            (temp_keys[0], self.tile_2),
            (temp_keys[1], self.tile_2)
        ]

        temp_bounds = Bounds(temp_keys[0], temp_keys[1])

        temp_md = Metadata(bounds=temp_bounds,
                           crs=self.md_proj,
                           cell_type=self.ct,
                           extent=self.extent,
                           layout_definition=self.ld)

        rdd = self.pysc.parallelize(temp_key_layer)
        layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, temp_md)

        actual = layer.merge()

        self.assertEqual(actual.srdd.rdd().count(), 2)

        for k, v in actual.to_numpy_rdd().collect():
            self.assertTrue((v.cells == self.arr_2).all())

if __name__ == "__main__":
    unittest.main()
