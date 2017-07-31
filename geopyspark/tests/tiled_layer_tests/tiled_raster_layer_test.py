import unittest
import pytest

from geopyspark.geotrellis.constants import LayerType
from geopyspark.tests.python_test_utils import geotiff_test_path
from geopyspark.geotrellis import Extent, LayoutDefinition, GlobalLayout, crs_to_proj4
from geopyspark.geotrellis.geotiff import get
from geopyspark.tests.base_test_class import BaseTestClass


class TiledRasterLayerTest(BaseTestClass):
    dir_path = geotiff_test_path("all-ones.tif")
    result = get(LayerType.SPATIAL, dir_path)
    tiled_layer = result.tile_to_layout()

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_tile_to_layout_layout_definition(self):
        layout_definition = self.tiled_layer.layer_metadata.layout_definition
        new_extent = Extent(layout_definition.extent.xmin,
                            layout_definition.extent.ymin,
                            layout_definition.extent.xmax + 15.0,
                            layout_definition.extent.ymax + 15.0)

        new_layout_definition = LayoutDefinition(extent=new_extent, tileLayout=layout_definition.tileLayout)

        actual = self.tiled_layer.tile_to_layout(new_layout_definition).layer_metadata.layout_definition.extent

        self.assertEqual(actual, new_extent)

    def test_tile_to_layout_tiled_layer(self):
        actual = self.tiled_layer.tile_to_layout(self.tiled_layer).layer_metadata
        expected = self.tiled_layer.layer_metadata

        self.assertDictEqual(actual.to_dict(), expected.to_dict())

    def test_tile_to_layout_global_layout(self):
        actual = self.tiled_layer.tile_to_layout(layout=GlobalLayout(zoom=5))

        self.assertEqual(actual.zoom_level, 5)

    def test_tile_to_layout_with_reproject(self):
        proj4 = crs_to_proj4(3857)
        actual = self.result.tile_to_layout(layout=GlobalLayout(), target_crs=proj4).layer_metadata.crs

        self.assertEqual(proj4, actual)

    def test_tile_to_layout_bad_crs(self):
        with pytest.raises(ValueError):
            self.result.tile_to_layout(layout=self.tiled_layer, target_crs=3857)


if __name__ == "__main__":
    unittest.main()
