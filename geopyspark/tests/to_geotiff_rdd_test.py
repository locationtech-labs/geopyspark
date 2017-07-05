import unittest
from os import walk, path
import rasterio
import pytest

from geopyspark.geotrellis.constants import LayerType
from geopyspark.tests.python_test_utils import geotiff_test_path
from geopyspark.geotrellis.geotiff import get
from geopyspark.tests.base_test_class import BaseTestClass


class ToGeoTiffTest(BaseTestClass):
    dir_path = geotiff_test_path("srtm_52_11.tif")
    rdd = get(BaseTestClass.pysc, LayerType.SPATIAL, dir_path)
    metadata = rdd.collect_metadata()

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_to_geotiff_rdd_rasterlayer(self):
        geotiff_rdd = self.rdd.to_geotiff_rdd(self.metadata, rows_per_strip=256)


if __name__ == "__main__":
    unittest.main()
