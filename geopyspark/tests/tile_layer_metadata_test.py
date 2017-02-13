from geopyspark.tests.python_test_utils import *
add_spark_path()
check_directory()

from pyspark import SparkContext
from geopyspark.geotrellis.tile_layer_metadata import TileLayerMetadata
from geopyspark.geotrellis.geotiff_rdd import HadoopGeoTiffRDD
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer

import unittest
import pytest


class TileLayerMetadataTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="metadata-test")
        self.metadata = TileLayerMetadata(self.pysc)
        self.hadoop_geotiff = HadoopGeoTiffRDD(self.pysc)

        self.dir_path = geotiff_test_path("all-ones.tif")
        self.options = {'maxTileSize': 256}

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.pysc.stop()
        self.pysc._gateway.close()

    def check_results(self, actual, expected):
        self.assertEqual(actual, expected)

    def test_collection(self):
        (rdd, schema) = self.hadoop_geotiff.get_spatial(self.dir_path)
        value = rdd.collect()[0]

        projected_extent = value[0]
        old_extent = projected_extent.extent

        new_extent = (old_extent.xmin, old_extent.ymin, old_extent.xmax, old_extent.ymax)

        (rows, cols) = value[1].shape
        layout = (1, 1, cols, rows)

        print(value[1].dtype)

        actual = (value[1].dtype, (new_extent, (layout)), new_extent)

        result = self.metadata.collect_python_metadata(rdd,
                                                       schema,
                                                       new_extent,
                                                       layout,
                                                       epsg_code=value[0].epsg_code)

        expected = (result['cellType'], result['layout'], result['extent'])

        self.check_results(actual, expected)


if __name__ == "__main__":
    unittest.main()
