from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.geotrellis.extent import Extent
from geopyspark.geotrellis.projected_extent import ProjectedExtent
from geopyspark.avroregistry import AvroRegistry

import unittest
import pytest


class ProjectedExtentSchemaTest(unittest.TestCase):
    def setUp(self):
        self.pysc = SparkContext(master="local[*]", appName="projectedextent-test")
        self.path = "geopyspark.geotrellis.tests.schemas.ProjectedExtentWrapper"
        java_import(self.pysc._gateway.jvm, self.path)

        self.extents = [Extent(0, 0, 1, 1), Extent(1, 2, 3, 4), Extent(5, 6, 7, 8)]

    @pytest.fixture(autouse=True)
    def tearDown(self):
        yield
        self.pysc.stop()
        self.pysc._gateway.close()

    def get_rdd(self):
        sc = self.pysc._jsc.sc()
        ew = self.pysc._gateway.jvm.ProjectedExtentWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_pextents(self):
        (pextents, schema) = self.get_rdd()

        return pextents.collect()

    def test_encoded_pextents(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: AvroRegistry.projected_extent_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
                {'epsg': 2004, 'extent': AvroRegistry.extent_encoder(self.extents[0])},
                {'epsg': 2004, 'extent': AvroRegistry.extent_encoder(self.extents[1])},
                {'epsg': 2004, 'extent': AvroRegistry.extent_encoder(self.extents[2])}
                ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_pextents(self):
        actual_pextents = self.get_pextents()

        expected_pextents = [
                ProjectedExtent(self.extents[0], 2004),
                ProjectedExtent(self.extents[1], 2004),
                ProjectedExtent(self.extents[2], 2004)
                ]

        for actual, expected in zip(actual_pextents, expected_pextents):
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
