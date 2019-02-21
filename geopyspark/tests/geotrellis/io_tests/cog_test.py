import unittest
import os
import pytest

from shapely.geometry import box

from geopyspark.geotrellis import Extent, SpatialKey, GlobalLayout, LocalLayout
from geopyspark.geotrellis.catalog import read_value, query, read_layer_metadata, AttributeStore
from geopyspark.geotrellis.constants import LayerType
from geopyspark.geotrellis.geotiff import get
from geopyspark.tests.base_test_class import BaseTestClass
from geopyspark.tests.python_test_utils import file_path


class COGTest(BaseTestClass):
    dir_path = file_path("catalog/")
    uri = "file://{}".format(dir_path)
    layer_name = "cog-layer"

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        BaseTestClass.pysc._gateway.close()

    def test_read_value(self):
        tiled = read_value(self.uri,
                           self.layer_name,
                           11,
                           1450,
                           966)

        self.assertEqual(tiled.cells.shape, (1, 256, 256))

    def test_bad_read_value(self):
        tiled = read_value(self.uri,
                           self.layer_name,
                           11,
                           1450,
                           2000)

        self.assertEqual(tiled, None)

    @pytest.mark.skipif('TRAVIS_PYTHON_VERSION' in os.environ.keys(),
                        reason="test_query does not pass on Travis")
    def test_query(self):
        intersection = box(74.88280541992188, 9.667967675781256, 75.05858666503909, 10.019530136718743)
        queried = query(self.uri, self.layer_name, 11, intersection)
        result = queried.count()

        self.assertEqual(result, 4)

    def test_read_metadata(self):
        layer = query(self.uri, self.layer_name, 5)
        actual_metadata = layer.layer_metadata

        expected_metadata = read_layer_metadata(self.uri, self.layer_name, 5)

        self.assertEqual(actual_metadata.to_dict(), expected_metadata.to_dict())


if __name__ == "__main__":
    unittest.main()
