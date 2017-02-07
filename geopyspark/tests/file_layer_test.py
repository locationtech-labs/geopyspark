from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from pyspark import SparkContext
from shapely.geometry import Polygon
from geopyspark.geotrellis.catalog import FileCatalog

import sys
import calendar
import time
import unittest
import pytest
import os
import shutil


class FileLayerTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="file-layer-test")
        self.file_catalog = FileCatalog(self.pysc)
        self.path = geotiff_test_path("catalog/file/")
        self.name = "ned"
        self.zoom = 0

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.pysc.stop()
        self.pysc._gateway.close()

    def test_reader(self):
        result = self.file_catalog.query_spatial_singleband(self.path, self.name, self.zoom)
        result[0].collect()

    def test_writer(self):
        result = self.file_catalog.query_spatial_singleband(self.path, self.name, self.zoom)

        rdd = result[0]
        metadata = result[1]

        new_layer = self.name + "-" + str(calendar.timegm(time.gmtime()))
        new_path = os.path.join(self.path, new_layer)

        self.file_catalog.write_spatial_singleband(
                layer_name=new_layer,
                layer_zoom=self.zoom,
                rdd=rdd,
                metadata=metadata,
                path=new_path)

        shutil.rmtree(new_path)


if __name__ == "__main__":
    unittest.main()
