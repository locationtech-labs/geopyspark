import unittest
import datetime
import pytest
import numpy as np

from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis import ProjectedExtent, Extent, Tile
from geopyspark.geotrellis.layer import RasterLayer
from geopyspark.geotrellis.constants import LayerType


class WithNoDataTest(BaseTestClass):
    epsg_code = 3857
    extent = Extent(0.0, 0.0, 10.0, 10.0)
    projected_extent = ProjectedExtent(extent, epsg_code)

    arr = np.zeros((1, 16, 16))
    tile = Tile(arr, 'FLOAT', -500.0)

    rdd = BaseTestClass.pysc.parallelize([(projected_extent, tile)])

    layer = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)
    tiled_layer = layer.tile_to_layout()

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_with_no_data_raster_layers(self):
        no_data_layer = self.layer.with_no_data(-10)
        tile = no_data_layer.to_numpy_rdd().first()[1]

        self.assertEqual(tile.no_data_value, -10)

        metadata = no_data_layer.collect_metadata()

        self.assertEqual(metadata.cell_type, "float32ud-10.0")
        self.assertEqual(metadata.no_data_value, -10)

    def test_with_no_data_tiled_raster_layers(self):
        no_data_layer = self.tiled_layer.with_no_data(18)
        tile = no_data_layer.to_numpy_rdd().first()[1]

        self.assertEqual(tile.no_data_value, 18)

        metadata = no_data_layer.layer_metadata

        self.assertEqual(metadata.cell_type, "float32ud18.0")
        self.assertEqual(metadata.no_data_value, 18)


if __name__ == "__main__":
    unittest.main()
