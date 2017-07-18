import unittest
import pytest

from geopyspark.geotrellis.constants import LayerType
from geopyspark.tests.python_test_utils import geotiff_test_path
from geopyspark.geotrellis import Extent, LayoutDefinition
from geopyspark.geotrellis.geotiff import get
from geopyspark.tests.base_test_class import BaseTestClass


class TiledRasterLayerTest(BaseTestClass):
    dir_path = geotiff_test_path("all-ones.tif")
    result = get(BaseTestClass.pysc, LayerType.SPATIAL, dir_path)
    tiled_layer = result.to_tiled_layer()

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_tile_to_layout(self):
        layout_definition = self.tiled_layer.layer_metadata.layout_definition
        new_extent = Extent(layout_definition.extent.xmin,
                            layout_definition.extent.ymin,
                            layout_definition.extent.xmax + 15.0,
                            layout_definition.extent.ymax + 15.0)

        new_layout_definition = LayoutDefinition(extent=new_extent, tileLayout=layout_definition.tileLayout)

        actual = self.tiled_layer.tile_to_layout(new_layout_definition).layer_metadata.layout_definition.extent

        self.assertEqual(actual, new_extent)


if __name__ == "__main__":
    unittest.main()
