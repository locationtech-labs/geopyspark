from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.geotrellis.extent import Extent
from geopyspark.geotrellis.temporal_projected_extent import TemporalProjectedExtent
from geopyspark.avroregistry import AvroRegistry
from geopyspark.tests.base_test_class import BaseTestClass

import unittest
import pytest


class TemporalProjectedExtentSchemaTest(BaseTestClass):
    path = "geopyspark.geotrellis.tests.schemas.TemporalProjectedExtentWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)
    extents = [Extent(0, 0, 1, 1), Extent(1, 2, 3, 4), Extent(5, 6, 7, 8)]

    def get_rdd(self):
        sc = BaseTestClass.pysc._jsc.sc()
        ew = BaseTestClass.pysc._gateway.jvm.TemporalProjectedExtentWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def get_tpextents(self):
        (tpextents, schema) = self.get_rdd()

        return tpextents.collect()

    def test_encoded_tpextents(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: AvroRegistry.temporal_projected_extent_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
                {'instant': 0, 'epsg': 2004, 'extent': AvroRegistry.extent_encoder(self.extents[0])},
                {'instant': 1, 'epsg': 2004, 'extent': AvroRegistry.extent_encoder(self.extents[1])},
                {'instant': 2, 'epsg': 2004, 'extent': AvroRegistry.extent_encoder(self.extents[2])}
                ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_tpextents(self):
        actual_tpextents = self.get_tpextents()

        expected_tpextents = [
                TemporalProjectedExtent(self.extents[0], 2004, 0),
                TemporalProjectedExtent(self.extents[1], 2004, 1),
                TemporalProjectedExtent(self.extents[2], 2004, 2)
                ]

        for actual, expected in zip(actual_tpextents, expected_tpextents):
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
