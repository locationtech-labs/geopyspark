import pytest
import unittest

from shapely.geometry import Point
import geopyspark as gps
from geopyspark.tests.base_test_class import BaseTestClass

class KeyTransformTest(BaseTestClass):

    layout = gps.LayoutDefinition(gps.Extent(0,0,1,1), gps.TileLayout(5,5,2,2))

    def test_key_to_extent(self):
        kt_layout = gps.KeyTransform(self.layout)
        self.assertEqual(gps.Extent(0.0, 0.8, 0.2, 1.0), kt_layout.key_to_extent(gps.SpatialKey(0,0)))

        kt_local = gps.KeyTransform(gps.LocalLayout(2), extent=gps.Extent(0,0,1,1), dimensions=(10,10))
        self.assertEqual(gps.Extent(0.0, 0.8, 0.2, 1.0), kt_layout.key_to_extent(gps.SpatialKey(0,0)))

        kt_global = gps.KeyTransform(gps.GlobalLayout(zoom=1), crs=4326)
        nw_global_extent = kt_global.key_to_extent(gps.SpatialKey(0,0))
        self.assertTrue(abs(nw_global_extent.xmin + 180.0) <= 1e-4 and
                        abs(nw_global_extent.xmax) <= 1e-4 and
                        abs(nw_global_extent.ymin) <= 1e-4 and
                        abs(nw_global_extent.ymax -90) <= 1e-4
        )

    def test_extent_to_key(self):
        kt = gps.KeyTransform(self.layout)
        self.assertTrue(set(kt.extent_to_keys(gps.Extent(0,0,0.4,0.4))) == set([gps.SpatialKey(x,y) for x in [0,1] for y in [3,4]]))

    def test_geom_to_key(self):
        kt = gps.KeyTransform(self.layout)
        self.assertTrue(kt.geometry_to_keys(Point(0.1,0.1)) == [gps.SpatialKey(0,4)])
