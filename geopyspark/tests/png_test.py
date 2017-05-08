import os
import unittest
import rasterio
import numpy as np
import pytest

from geopyspark.geotrellis.constants import SPATIAL
from geopyspark.geotrellis.rdd import RasterRDD
from geopyspark.geotrellis.render import PngRDD
from geopyspark.tests.base_test_class import BaseTestClass


class PngRddTest(BaseTestClass):

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.geopysc.pysc._gateway.close()

    def test_if_working(self):
        arr = np.zeros((1, 16, 16))
        epsg_code = 3857
        extent = {'xmin': 0.0, 'ymin': 0.0, 'xmax': 10.0, 'ymax': 10.0}

        tile = {'data': arr, 'no_data_value': False}
        projected_extent = {'extent': extent, 'epsg': epsg_code}

        rdd = BaseTestClass.geopysc.pysc.parallelize([(projected_extent, tile)])
        raster_rdd = RasterRDD.from_numpy_rdd(BaseTestClass.geopysc, SPATIAL, rdd)

        laid_out = raster_rdd.to_tiled_layer()

        result = PngRDD(BaseTestClass.geopysc, SPATIAL, laid_out, "hot")


if __name__ == "__main__":
    unittest.main()
