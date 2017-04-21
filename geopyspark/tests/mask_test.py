import os
import unittest
import pytest
import rasterio
import numpy as np

from shapely.geometry import Polygon
from geopyspark.geotrellis.tile_layer import python_mask, geotrellis_mask
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import SPATIAL


class MaskTest(BaseTestClass):
    geopysc = BaseTestClass.geopysc

    data = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0]]])

    layer = [({'row': 0, 'col': 0}, {'no_data_value': -1.0, 'data': data}),
             ({'row': 1, 'col': 0}, {'no_data_value': -1.0, 'data': data}),
             ({'row': 0, 'col': 1}, {'no_data_value': -1.0, 'data': data}),
             ({'row': 1, 'col': 1}, {'no_data_value': -1.0, 'data': data})]
    rdd = geopysc.pysc.parallelize(layer)

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
                    'tileLayout': layout}}

    geometries = [Polygon([(17, 17), (42, 17), (42, 42), (17, 42)])]

    def test_python_mask(self):
        result = python_mask(self.rdd, self.metadata, self.geometries)
        n = result.map(lambda kv: np.sum(kv[1]['data'])).reduce(lambda a,b: a + b)
        self.assertEqual(n, -50)

    @pytest.mark.skipif('TRAVIS' in os.environ,
                        reason="Mysteriously fails on Travis")
    def test_geotrellis_mask(self):
        result = geotrellis_mask(geopysc=self.geopysc,
                                 rdd_type=SPATIAL,
                                 keyed_rdd=self.rdd,
                                 metadata=self.metadata,
                                 geometries=self.geometries)
        n = result.map(lambda kv: np.sum(kv[1]['data'])).reduce(lambda a,b: a + b)
        self.assertEqual(n, 25.0)

if __name__ == "__main__":
    unittest.main()
