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


class CollectKeysTest(BaseTestClass):
    arr = np.zeros((1, 4, 4))
    tile = Tile.from_numpy_array(arr)

    crs = 4326
    time = datetime.datetime.strptime("2016-08-24T09:00:00Z", '%Y-%m-%dT%H:%M:%SZ')

    extents = [
        Extent(0.0, 0.0, 4.0, 4.0),
        Extent(0.0, 4.0, 4.0, 8.0),
        Extent(4.0, 0.0, 8.0, 4.0),
        Extent(4.0, 4.0, 8.0, 8.0)
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
            ProjectedExtent(extent=self.extents[2], epsg=self.crs),
            ProjectedExtent(extent=self.extents[3], epsg=self.crs)
        ]

        pe_layer = [
            (pes[0], self.tile),
            (pes[1], self.tile),
            (pes[2], self.tile),
            (pes[3], self.tile)
        ]

        rdd = self.pysc.parallelize(pe_layer)
        layer = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)

        actual = layer.collect_keys()

        for x in actual:
            self.assertTrue(x in pes)

    def test_temporal_projected_extent(self):
        pes = [
            TemporalProjectedExtent(extent=self.extents[0], epsg=self.crs, instant=self.time),
            TemporalProjectedExtent(extent=self.extents[1], epsg=self.crs, instant=self.time),
            TemporalProjectedExtent(extent=self.extents[2], epsg=self.crs, instant=self.time),
            TemporalProjectedExtent(extent=self.extents[3], epsg=self.crs, instant=self.time)
        ]

        pe_layer = [
            (pes[0], self.tile),
            (pes[1], self.tile),
            (pes[2], self.tile),
            (pes[3], self.tile)
        ]

        rdd = self.pysc.parallelize(pe_layer)
        layer = RasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd)

        actual = layer.collect_keys()

        for x in actual:
            self.assertTrue(x in pes)

    def test_spatial_keys(self):
        keys = [
            SpatialKey(0, 0),
            SpatialKey(0, 1),
            SpatialKey(1, 0),
            SpatialKey(1, 1)
        ]

        key_layer = [
            (keys[0], self.tile),
            (keys[1], self.tile),
            (keys[2], self.tile),
            (keys[3], self.tile)
        ]

        bounds = Bounds(keys[0], keys[3])

        md = Metadata(bounds=bounds,
                      crs=self.md_proj,
                      cell_type=self.ct,
                      extent=self.extent,
                      layout_definition=self.ld)

        rdd = self.pysc.parallelize(key_layer)
        layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, md)

        actual = layer.collect_keys()

        for x in actual:
            self.assertTrue(x in keys)

    def test_space_time_keys(self):
        temp_keys = [
            SpaceTimeKey(0, 0, instant=self.time),
            SpaceTimeKey(0, 1, instant=self.time),
            SpaceTimeKey(1, 0, instant=self.time),
            SpaceTimeKey(1, 1, instant=self.time)
        ]

        temp_key_layer = [
            (temp_keys[0], self.tile),
            (temp_keys[1], self.tile),
            (temp_keys[2], self.tile),
            (temp_keys[3], self.tile)
        ]

        temp_bounds = Bounds(temp_keys[0], temp_keys[3])

        temp_md = Metadata(bounds=temp_bounds,
                           crs=self.md_proj,
                           cell_type=self.ct,
                           extent=self.extent,
                           layout_definition=self.ld)

        rdd = self.pysc.parallelize(temp_key_layer)
        layer = TiledRasterLayer.from_numpy_rdd(LayerType.SPACETIME, rdd, temp_md)

        actual = layer.collect_keys()

        for x in actual:
            self.assertTrue(x in temp_keys)

if __name__ == "__main__":
    unittest.main()
