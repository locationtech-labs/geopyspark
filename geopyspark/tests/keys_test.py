from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.tests.base_test_class import BaseTestClass

import unittest


class SpatialKeySchemaTest(BaseTestClass):
    path = "geopyspark.geotrellis.tests.schemas.SpatialKeyWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    expected_skey = {'col': 7, 'row': 3}

    def get_rdd(self):
        sc = BaseTestClass.pysc._jsc.sc()
        ew = BaseTestClass.pysc._gateway.jvm.SpatialKeyWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)
        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def test_encoded_skeys(self):
        (rdd, schema) = self.get_rdd()
        encoded = rdd.map(lambda s: s)

        actual_encoded = encoded.first()

        self.assertDictEqual(actual_encoded, self.expected_skey)

    def test_decoded_extents(self):
        (skeys, schema) = self.get_rdd()
        actual_skeys = skeys.first()

        self.assertDictEqual(actual_skeys, self.expected_skey)


class SpaceTimeKeySchemaTest(BaseTestClass):
    path = "geopyspark.geotrellis.tests.schemas.SpaceTimeKeyWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    expected_skeys = [
        {'col': 7, 'row': 3, 'instant': 5},
        {'col': 9, 'row': 4, 'instant': 10},
        {'col': 11, 'row': 5, 'instant': 15}
    ]

    def get_rdd(self):
        java_import(BaseTestClass.pysc._gateway.jvm, self.path)
        sc = BaseTestClass.pysc._jsc.sc()
        ew = BaseTestClass.pysc._gateway.jvm.SpaceTimeKeyWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)

        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def test_encoded_skeys(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: s)
        actual_encoded = encoded.collect()

        for actual, expected in zip(actual_encoded, self.expected_skeys):
            self.assertEqual(actual, expected)

    def test_decoded_extents(self):
        (skeys, schema) = self.get_rdd()
        actual_skeys = skeys.collect()

        for actual, expected in zip(actual_skeys, self.expected_skeys):
            self.assertDictEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
