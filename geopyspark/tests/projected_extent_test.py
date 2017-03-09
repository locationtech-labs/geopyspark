from geopyspark.tests.python_test_utils import add_spark_path
add_spark_path()

from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.tests.base_test_class import BaseTestClass

import unittest


class ProjectedExtentSchemaTest(BaseTestClass):
    path = "geopyspark.geotrellis.tests.schemas.ProjectedExtentWrapper"
    java_import(BaseTestClass.pysc._gateway.jvm, path)

    projected_extents = [
        {'epsg': 2004, 'extent': {'xmin': 0, 'ymin': 0, 'xmax': 1, 'ymax': 1}},
        {'epsg': 2004, 'extent': {'xmin': 1, 'ymin': 2, 'xmax': 3, 'ymax': 4}},
        {'epsg': 2004, 'extent': {'xmin': 5, 'ymin': 6, 'xmax': 7, 'ymax': 8}}]

    def get_rdd(self):
        sc = BaseTestClass.pysc._jsc.sc()
        ew = BaseTestClass.pysc._gateway.jvm.ProjectedExtentWrapper

        tup = ew.testOut(sc)
        (java_rdd, schema) = (tup._1(), tup._2())

        ser = AvroSerializer(schema)

        return (RDD(java_rdd, BaseTestClass.pysc, AutoBatchedSerializer(ser)), schema)

    def test_encoded_pextents(self):
        (rdd, schema) = self.get_rdd()

        encoded = rdd.map(lambda s: s)
        actual_encoded = encoded.collect()

        for actual, expected in zip(actual_encoded, self.projected_extents):
            self.assertDictEqual(actual, expected)

    def test_decoded_pextents(self):
        (pextents, schema) = self.get_rdd()
        actual_pextents = pextents.collect()

        for actual, expected in zip(actual_pextents, self.projected_extents):
            self.assertDictEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
