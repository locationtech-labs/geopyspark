import numpy as np
import os
import pytest
import unittest

from geopyspark.geotrellis import SpatialKey, Extent, Tile
from geopyspark.geotrellis.layer import TiledRasterLayer
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import LayerType, Operation, Neighborhood
from geopyspark.geotrellis.neighborhood import Square, Annulus, Wedge, Circle, Nesw


class FocalTest(BaseTestClass):
    cells = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 0.0]]])

    tile = Tile.from_numpy_array(cells, -1.0)

    layer = [(SpatialKey(0, 0), tile),
             (SpatialKey(1, 0), tile),
             (SpatialKey(0, 1), tile),
             (SpatialKey(1, 1), tile)]
    rdd = BaseTestClass.pysc.parallelize(layer)

    extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 33.0, 'ymax': 33.0}
    layout = {'layoutCols': 2, 'layoutRows': 2, 'tileCols': 5, 'tileRows': 5}
    metadata = {'cellType': 'float32ud-1.0',
                'extent': extent,
                'crs': '+proj=longlat +datum=WGS84 +no_defs ',
                'bounds': {
                    'minKey': {'col': 0, 'row': 0},
                    'maxKey': {'col': 1, 'row': 1}},
                'layoutDefinition': {
                    'extent': extent,
                    'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 2, 'layoutRows': 2}}}

    raster_rdd = TiledRasterLayer.from_numpy_rdd(LayerType.SPATIAL, rdd, metadata)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_focal_sum_square(self):
        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=Neighborhood.SQUARE,
            param_1=1.0)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 6)

    def test_focal_sum_wedge(self):
        neighborhood = Wedge(radius=1.0, start_angle=0.0, end_angle=180.0)
        self.assertEqual(str(neighborhood), repr(neighborhood))

        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=neighborhood)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 3)

    def test_focal_sum_circle(self):
        neighborhood = Circle(radius=1.0)
        self.assertEqual(str(neighborhood), repr(neighborhood))

        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=neighborhood)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 4)

    def test_focal_sum_nesw(self):
        neighborhood = Nesw(extent=1.0)
        self.assertEqual(str(neighborhood), repr(neighborhood))

        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=neighborhood)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 4)

    def test_focal_sum_annulus(self):
        neighborhood = Annulus(inner_radius=0.5, outer_radius=1.5)
        self.assertEqual(str(neighborhood), repr(neighborhood))

        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=neighborhood)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 5.0)

    def test_square(self):
        neighborhood = Square(extent=1.0)
        self.assertEqual(str(neighborhood), repr(neighborhood))

        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=neighborhood)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 6.0)

    def test_focal_sum_int(self):
        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=Neighborhood.SQUARE,
            param_1=1)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 6)

    def test_focal_sum_square(self):
        square = Square(extent=1.0)
        result = self.raster_rdd.focal(
            operation=Operation.SUM,
            neighborhood=square)

        self.assertTrue(result.to_numpy_rdd().first()[1].cells[0][1][0] >= 6)

    def test_focal_min(self):
        result = self.raster_rdd.focal(operation=Operation.MIN, neighborhood=Neighborhood.ANNULUS,
                                       param_1=2.0, param_2=1.0)

        self.assertEqual(result.to_numpy_rdd().first()[1].cells[0][0][0], -1)

    def test_focal_min_annulus(self):
        annulus = Annulus(inner_radius=2.0, outer_radius=1.0)
        result = self.raster_rdd.focal(operation=Operation.MIN, neighborhood=annulus)

        self.assertEqual(result.to_numpy_rdd().first()[1].cells[0][0][0], -1)

    def test_focal_min_int(self):
        result = self.raster_rdd.focal(operation=Operation.MIN, neighborhood=Neighborhood.ANNULUS,
                                       param_1=2, param_2=1)

        self.assertEqual(result.to_numpy_rdd().first()[1].cells[0][0][0], -1)


if __name__ == "__main__":
    unittest.main()
    BaseTestClass.pysc.stop()
