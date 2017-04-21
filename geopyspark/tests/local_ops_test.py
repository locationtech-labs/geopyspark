import unittest
import rasterio
import pytest

from geopyspark.geotrellis.constants import SPATIAL
from geopyspark.tests.python_test_utils import geotiff_test_path
from geopyspark.geotrellis.geotiff_rdd import get
from geopyspark.tests.base_test_class import BaseTestClass


class LocalOpertaionsTest(BaseTestClass):
    dir_path = geotiff_test_path("all-ones.tif")
    result = get(BaseTestClass.geopysc, SPATIAL, dir_path)
    md = result.collect_metadata()
    tiled = result.tile_to_layout(md)

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.geopysc.pysc._gateway.close()

    def test_local_int(self, option=None):
        actual = self.tiled + 1
        print(actual.to_numpy_rdd().first()[1]['data'])

if __name__ == "__main__":
    unittest.main()
