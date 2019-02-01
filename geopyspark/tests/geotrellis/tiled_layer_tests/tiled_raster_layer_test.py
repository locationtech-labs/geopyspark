import unittest
import pytest

from geopyspark.geotrellis.constants import LayerType, ReadMethod
from geopyspark.geotrellis import Extent, GlobalLayout, LocalLayout, SourceInfo
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.layer import TiledRasterLayer


class TiledRasterLayerTest(BaseTestClass):
    difference = 0.000001

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def read(self, layout, read_method, target_crs=None, multiplex=False):
        expected_tiled = self.rdd.tile_to_layout(layout, target_crs=target_crs)
        expected_collected = expected_tiled.to_numpy_rdd().collect()

        if multiplex:
            sources = [SourceInfo(self.path, {0: 0}), SourceInfo(self.path, {0:1})]
            actual_tiled = TiledRasterLayer.read(sources,
                                                 layout_type=layout,
                                                 target_crs=target_crs)
        else:
            actual_tiled = TiledRasterLayer.read([self.path],
                                                 layout_type=layout,
                                                 target_crs=target_crs)

        actual_collected = actual_tiled.to_numpy_rdd().collect()

        self.assertEqual(len(expected_collected), len(actual_collected))

        expected_collected.sort(key=lambda tup: (tup[0].col, tup[0].row))
        actual_collected.sort(key=lambda tup: (tup[0].col, tup[0].row))

        if multiplex:
            bands = (0, 1)
        else:
            bands = [0]

        for expected, actual in zip(expected_collected, actual_collected):
            for x in bands:
                self.assertEqual(expected[0], actual[0])
                self.assertTrue(expected[1].cells.shape[1:] == actual[1].cells[x,:,:].shape)

                diff = abs(expected[1].cells - actual[1].cells[x,:,:])
                off_values_count = (diff > self.difference).sum()

                self.assertTrue(off_values_count / expected[1].cells.size <= 0.025)

    # Tests using LocalLayout

    def test_read_no_reproject_local_geotrellis(self):
        self.read(LocalLayout(256, 256), ReadMethod.GEOTRELLIS)

    def test_read_ordered_no_reproject_local_geotrellis(self):
        self.read(LocalLayout(256, 256), ReadMethod.GEOTRELLIS, multiplex=True)

    def test_read_no_reproject_local_gdal(self):
        self.read(LocalLayout(256, 256), ReadMethod.GDAL)

    def test_read_with_reproject_local_geotrellis(self):
        self.read(LocalLayout(128, 256), ReadMethod.GEOTRELLIS, target_crs=3857)

    def test_ordered_read_with_reproject_local_geotrellis(self):
        self.read(LocalLayout(128, 256), ReadMethod.GEOTRELLIS, target_crs=3857, multiplex=True)

    def test_read_with_reproject_local_gdal(self):
        self.read(LocalLayout(128, 256), ReadMethod.GDAL, target_crs=3857)

    # Tests with GlobalLayout

    def test_read_no_reproject_global_geotrellis(self):
        self.read(GlobalLayout(tile_size=16, zoom=4), ReadMethod.GEOTRELLIS)

    def test_ordered_read_no_reproject_global_geotrellis(self):
        self.read(GlobalLayout(tile_size=16, zoom=4), ReadMethod.GEOTRELLIS, multiplex=True)

    def test_read_no_reproject_global_gdal(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GDAL)

    def test_read_with_reproject_global_geotrellis(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GEOTRELLIS, target_crs=3857)

    def test_read_with_reproject_global_gdal(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GDAL, target_crs=3857)

    def test_ordered_read_with_reproject_global_geotrellis(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GEOTRELLIS, target_crs=3857, multiplex=True)

    def test_read_with_reproject_global_gdal(self):
        self.read(GlobalLayout(tile_size=128, zoom=4), ReadMethod.GDAL, target_crs=3857)


if __name__ == "__main__":
    unittest.main()
