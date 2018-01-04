import os
import unittest
import rasterio
import pytest

from geopyspark.geotrellis import Tile, LayoutDefinition, LocalLayout
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.layer import RasterLayer
from geopyspark.tests.base_test_class import BaseTestClass


class TileLayerMetadataTest(BaseTestClass):
    extent = BaseTestClass.extent
    layout = BaseTestClass.layout
    layout_def = LayoutDefinition(extent, layout)
    rdd = BaseTestClass.rdd
    projected_extent = BaseTestClass.projected_extent
    cols = BaseTestClass.cols

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_collection_avro_rdd(self):
        result = self.rdd.collect_metadata(layout=self.layout_def)

        self.assertEqual(result.extent, self.extent)
        self.assertEqual(result.layout_definition.extent, self.extent)
        self.assertEqual(result.layout_definition.tileLayout, self.layout)

    @pytest.mark.skipif('TRAVIS' in os.environ,
                        reason="Test causes memory errors on Travis")
    def test_collection_python_rdd(self):
        data = rasterio.open(self.dir_path)
        tile_dict = Tile(data.read(), 'FLOAT', data.nodata)

        rasterio_rdd = self.pysc.parallelize([(self.projected_extent, tile_dict)])
        raster_rdd = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rasterio_rdd)

        result = raster_rdd.collect_metadata(layout=self.layout_def)

        self.assertEqual(result.extent, self.extent)
        self.assertEqual(result.layout_definition.extent, self.extent)
        self.assertEqual(result.layout_definition.tileLayout, self.layout)

    def test_collection_floating(self):
        result = self.rdd.collect_metadata(LocalLayout(self.cols))

        self.assertEqual(result.extent, self.extent)
        self.assertEqual(result.layout_definition.extent, self.extent)
        self.assertEqual(result.layout_definition.tileLayout, self.layout)


if __name__ == "__main__":
    unittest.main()
