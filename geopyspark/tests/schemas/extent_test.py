from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.geotrellis.extent import Extent
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry

import unittest

class ExtentSchemaTest(unittest.TestCase):
    pysc = SparkContext(master="local", appName="extent-test")
    path = "geopyspark.geotrellis.tests.schemas.ExtentWrapper"
    java_import(pysc._gateway.jvm, path)

    def get_rdd(self):
        sc = self.pysc._jsc.sc()
        ew = self.pysc._gateway.jvm.ExtentWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser)), schema)

    def get_extents(self):
        (extents, schema) = self.get_rdd()

        return extents.collect()

    def test_encoded_extents(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: AvroRegistry.extent_encoder(s))
        actual_encoded = encoded.collect()

        expected_encoded = [
                {'xmin': 0, 'ymin': 0, 'xmax': 1, 'ymax': 1},
                {'xmin': 1, 'ymin': 2, 'xmax': 3, 'ymax': 4},
                {'xmin': 5, 'ymin': 6, 'xmax': 7, 'ymax': 8}
                ]

        for actual, expected in zip(actual_encoded, expected_encoded):
            self.assertEqual(actual, expected)

    def test_decoded_extents(self):
        actual_extents = self.get_extents()

        expected_extents = [
                Extent(0, 0, 1, 1),
                Extent(1, 2, 3, 4),
                Extent(5, 6, 7, 8)
                ]

        for actual, expected in zip(actual_extents, expected_extents):
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
