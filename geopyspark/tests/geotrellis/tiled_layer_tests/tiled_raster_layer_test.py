import unittest
import pytest

from geopyspark.geotrellis.constants import LayerType, ReadMethod
from geopyspark.geotrellis import Extent, GlobalLayout, LocalLayout
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import TiledRasterLayer


class TiledRasterLayerTest(BaseTestClass):
    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def read(self, layout, read_method, target_crs=None):
        expected_tiled = self.rdd.tile_to_layout(layout, target_crs=target_crs)
        expected_collected = expected_tiled.to_numpy_rdd().collect()

        actual_tiled = TiledRasterLayer.read_to_layout([self.path],
                                                       layout_type=layout,
                                                       target_crs=target_crs)

        actual_collected = actual_tiled.to_numpy_rdd().collect()

        self.assertEqual(len(expected_collected), len(actual_collected))

        expected_collected.sort(key=lambda tup: (tup[0].col, tup[0].row))
        actual_collected.sort(key=lambda tup: (tup[0].col, tup[0].row))

        for expected, actual in zip(expected_collected, actual_collected):
            self.assertEqual(expected[0], actual[0])
            self.assertTrue((expected[1].cells == actual[1].cells).all())

    # Tests using LocalLayout

    def test_read_no_reproject_local_geotrellis(self):
        self.read(LocalLayout(256, 256), ReadMethod.GEOTRELLIS)

    def test_read_no_reproject_local_gdal(self):
        self.read(LocalLayout(256, 256), ReadMethod.GDAL)

    def test_read_with_reproject_local_geotrellis(self):
        self.read(LocalLayout(128, 256), ReadMethod.GEOTRELLIS, target_crs=3857)

    def test_read_with_reproject_local_gdal(self):
        self.read(LocalLayout(128, 256), ReadMethod.GDAL, target_crs=3857)

    # Tests with GlobalLayout

    def test_read_no_reproject_global_geotrellis(self):
        self.read(GlobalLayout(tile_size=16, zoom=4), ReadMethod.GEOTRELLIS)

    def test_read_no_reproject_global_gdal(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GDAL)

    def test_read_with_reproject_global_geotrellis(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GEOTRELLIS, target_crs=3857)

    def test_read_with_reproject_global_gdal(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GDAL, target_crs=3857)



if __name__ == "__main__":
    unittest.main()
