import os
import unittest
import pytest

from geopyspark.geotrellis import LayoutDefinition, LocalLayout, GlobalLayout
from geopyspark.tests.base_test_class import BaseTestClass


class ReprojectTest(BaseTestClass):
    layout_def = LayoutDefinition(BaseTestClass.extent, BaseTestClass.layout)
    metadata = BaseTestClass.rdd.collect_metadata(layout=layout_def)

    crs = metadata.crs
    expected_crs = "+proj=longlat +ellps=WGS72 +towgs84=0,0,1.9,0,0,0.814,-0.38 +no_defs "

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_repartition(self):
        laid_out_rdd = BaseTestClass.rdd.tile_to_layout(self.metadata)
        result = laid_out_rdd.repartition(2)
        self.assertEqual(result.getNumPartitions(), 2)

    def test_same_crs_layout(self):
        result = BaseTestClass.rdd.tile_to_layout(layout=self.layout_def, target_crs="EPSG:4326")
        new_metadata = result.layer_metadata

        layout_definition = LayoutDefinition(BaseTestClass.extent, BaseTestClass.layout)

        self.assertEqual(layout_definition, new_metadata.layout_definition)

    def test_integer_crs(self):
        result = BaseTestClass.rdd.tile_to_layout(layout=self.layout_def, target_crs=4326)
        new_metadata = result.layer_metadata

        layout_definition = LayoutDefinition(BaseTestClass.extent, BaseTestClass.layout)

        self.assertEqual(layout_definition, new_metadata.layout_definition)

    def test_same_crs_zoom(self):
        result = BaseTestClass.rdd.tile_to_layout(layout=LocalLayout(self.cols), target_crs="EPSG:4326")
        new_metadata = result.layer_metadata

        self.assertTrue("+datum=WGS84" in new_metadata.crs)

    def test_different_crs_layout(self):
        result = BaseTestClass.rdd.tile_to_layout(layout=self.layout_def, target_crs="EPSG:4324")
        new_metadata = result.layer_metadata

        self.assertEqual(self.expected_crs, new_metadata.crs)

    def test_different_crs_zoom(self):
        result = BaseTestClass.rdd.tile_to_layout(layout=GlobalLayout(tile_size=self.cols),
                                                  target_crs="EPSG:4324")
        new_metadata = result.layer_metadata

        self.assertEqual(self.expected_crs, new_metadata.crs)

    def test_different_crs_float(self):
        result = BaseTestClass.rdd.tile_to_layout(layout=LocalLayout(self.cols), target_crs="EPSG:4324")
        new_metadata = result.layer_metadata

        self.assertEqual(self.expected_crs, new_metadata.crs)


if __name__ == "__main__":
    unittest.main()
