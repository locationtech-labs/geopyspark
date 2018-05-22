import unittest
import pytest
import os

from geopyspark import create_python_rdd

from geopyspark.tests.python_test_utils import file_path
from geopyspark.tests.base_test_class import BaseTestClass

from geopyspark.geotrellis.protobufserializer import ProtoBufSerializer

from geopyspark.geotools.shapefile import get
from geopyspark.geotools.geotools_protobufcodecs import feature_decoder


class HadoopShapefileReadingTest(BaseTestClass):
    dir_path_1 = file_path("shapefiles/demographics/")
    dir_path_2 = file_path("shapefiles/WBDHU4.shp")

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    @pytest.mark.skipif('TRAVIS' in os.environ,
                         reason="A shapeless method cannot be accessed for some reason on Travis")
    def test_reading_files_with_non_empty_attributes(self):
        result = get(self.dir_path_1).collect()

        self.assertEqual(len(result), 160)

        expected_keys = set(['ename', 'gbcode', 'Employment', 'LowIncome', 'WorkingAge', 'TotalPop'])

        for feature in result:
            self.assertEqual(set(feature.properties.keys()), expected_keys)

    @pytest.mark.skipif('TRAVIS' in os.environ,
                         reason="A shapeless method cannot be accessed for some reason on Travis")
    def test_reading_files_with_empty_attributes(self):
        result = get(self.dir_path_2).collect()

        self.assertEqual(len(result), 1)

if __name__ == "__main__":
    unittest.main()
