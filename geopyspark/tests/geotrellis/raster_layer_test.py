import os
import unittest
import numpy as np
import pytest

from geopyspark.geotrellis import (SpatialKey,
                                   Tile,
                                   ProjectedExtent,
                                   Extent,
                                   RasterLayer,
                                   LocalLayout,
                                   TileLayout,
                                   GlobalLayout,
                                   LayoutDefinition,
                                   SpatialPartitionStrategy)
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import LayerType, CellType, ReadMethod


def make_raster(x, y, v, cols=4, rows=4, ct=CellType.FLOAT32, crs=4326):
    cells = np.zeros((1, rows, cols))
    cells.fill(v)
    # extent of a single cell is 1, no fence-post here
    extent = ProjectedExtent(Extent(x, y, x + cols, y + rows), crs)
    return (extent, Tile(cells, ct, None))


class RasterLayerTest(BaseTestClass):
    layers = [
        make_raster(0, 0, v=1),
        make_raster(3, 2, v=2),
        make_raster(6, 0, v=3)
    ]

    # TODO: Have Travis be able to run the GDAL tests

    numpy_rdd = BaseTestClass.pysc.parallelize(layers)
    layer = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, numpy_rdd)
    metadata = layer.collect_metadata(GlobalLayout(5))

    def read_no_reproject(self, read_method):
        actual_raster_layer = RasterLayer.read([self.path], read_method=read_method)

        collected = actual_raster_layer.to_numpy_rdd().first()

        (projected_extent, tile) = collected

        self.assertEqual(projected_extent.extent, self.extent)
        self.assertEqual(projected_extent.proj4, self.projected_extent.proj4)

        self.assertTrue((self.expected_tile == tile.cells).all())

    def read_with_reproject(self, read_method):
        expected_raster_layer = self.rdd.reproject(target_crs=3857)

        expected_collected = expected_raster_layer.to_numpy_rdd().first()
        (expected_projected_extent, expected_tile) = expected_collected

        actual_raster_layer = RasterLayer.read([self.path], target_crs=3857, read_method=read_method)

        actual_collected = actual_raster_layer.to_numpy_rdd().first()
        (actual_projected_extent, actual_tile) = actual_collected

        self.assertEqual(actual_projected_extent.epsg, expected_projected_extent.epsg)

        self.assertTrue((expected_tile.cells == actual_tile.cells).all())

    def test_read_no_reproject_geotrellis(self):
        self.read_no_reproject(ReadMethod.GEOTRELLIS)

    @pytest.mark.skipif('TRAVIS' in os.environ,
                        reason="Travis does not currently support GDAL")
    def test_read_no_reproject_gdal(self):
        self.read_no_reproject(ReadMethod.GDAL)

    def test_read_with_reproject_geotrellis(self):
        self.read_with_reproject(ReadMethod.GEOTRELLIS)

    @pytest.mark.skipif('TRAVIS' in os.environ,
                        reason="Travis does not currently support GDAL")
    def test_read_with_reproject_gdal(self):
        self.read_with_reproject(ReadMethod.GDAL)

    def test_no_data_of_zero(self):
        no_data_layer = [(t[0], Tile.from_numpy_array(t[1].cells, 1)) for t in self.layers]

        rdd = BaseTestClass.pysc.parallelize(no_data_layer)
        nd_layer = RasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd)
        nd_metadata = nd_layer.collect_metadata()

        self.assertTrue('ud1' in nd_metadata.cell_type)
        self.assertEqual(nd_metadata.no_data_value, 1)

    @pytest.fixture(scope='class', autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()


if __name__ == "__main__":
    unittest.main()
