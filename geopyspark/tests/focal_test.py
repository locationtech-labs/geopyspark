import os
import unittest
import rasterio
import numpy as np

from geopyspark.tests.python_test_utils import check_directory, geotiff_test_path
from geopyspark.geotrellis.tile_layer import collect_metadata, focal
from geopyspark.geotrellis.geotiff_rdd import geotiff_rdd
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.geotrellis.constants import SPATIAL, SUM, MIN, SQUARE, ANNULUS



class FocalTest(BaseTestClass):
    geopysc = BaseTestClass.geopysc

    data = np.array([[
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 1.0],
        [1.0, 1.0, 1.0, 1.0, 0.0]]])

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
                    'tileLayout': {'tileCols': 5, 'tileRows': 5, 'layoutCols': 2, 'layoutRows': 2}}}

    def test_focal_sum(self):
        result = focal(geopysc=self.geopysc,
                       rdd_type=SPATIAL,
                       keyed_rdd=self.rdd,
                       metadata=self.metadata,
                       op=SUM,
                       neighborhood=SQUARE,
                       param1=1.0)

        self.assertTrue(result.first()[1]['data'][0][1][0] >= 6)

    def test_focal_min(self):
        result = focal(geopysc=self.geopysc,
                       rdd_type=SPATIAL,
                       keyed_rdd=self.rdd,
                       metadata=self.metadata,
                       op=MIN,
                       neighborhood=ANNULUS,
                       param1=2.0, param2=1.0)

        self.assertEqual(result.first()[1]['data'][0][0][0], -1)

if __name__ == "__main__":
    unittest.main()
