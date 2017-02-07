from geopyspark.tests.python_test_utils import *
add_spark_path()

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
        self.path = "/tmp/catalog/"
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

    '''
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
    '''


if __name__ == "__main__":
    unittest.main()


    '''
    if len(sys.argv) > 3:
        path = sys.argv[1]
        layer_name = sys.argv[2]
        layer_zoom = int(sys.argv[3])
    else:
        exit(-1)

    sc = SparkContext(appName="file-layer-test")
    catalog = FileCatalog(path, sc)
    (rdd, metadata) = catalog.query("spatial", "singleband", layer_name, layer_zoom)
    new_layer_name = layer_name + "-" + str(calendar.timegm(time.gmtime()))
    catalog.write(new_layer_name, layer_zoom, rdd, metadata)
    '''
