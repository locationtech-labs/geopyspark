from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import

import io
import zlib
import struct
import sys
import avro
import avro.io
import binascii
import StringIO


class AvroSerializer(FramedSerializer):

    def __init__(self, schema):
        self.schema = avro.schema.parse(schema)
        self.reader = avro.io.DatumReader(self.schema)
        self.datum_writer = avro.io.DatumWriter(self.schema)

    """
    Deserializes a byte array into an object.
    """
    def dumps(self, obj):
        writer = StringIO.StringIO()
        encoder = avro.io.BinaryEncoder(writer)
        datum = {
            'col': obj[0].col,
            'row': obj[0].row
        }
        self.datum_writer.write(datum, encoder)
        return writer.getvalue()


    """
    Serialize an object into a byte array.
    When batching is used, this will be called with an array of objects.
    """

    def loads(self, obj):
        buf = io.BytesIO(obj)
        decoder = avro.io.BinaryDecoder(buf)
        i = self.reader.read(decoder)
        return [SpatialKey(i.get('col'), i.get('row'))]


if __name__ == "__main__":
    sys.path.append("dependencies/python_spatial_key.zip")
    from spatial_key import SpatialKey

    schema = """{"type":"record","name":"SpatialKey","namespace":"geotrellis.spark","fields":[{"name":"col","type":"int"},{"name":"row","type":"int"}]}"""
    ser = AvroSerializer(schema)
    sc = SparkContext(master="local", appName="python-test")

    java_import(sc._gateway.jvm, "geopyspark.geotrellis.Box")

    testRdd = sc._gateway.jvm.Box.testRdd(sc._jsc.sc())
    encodedRdd = sc._gateway.jvm.Box.encodeRdd(testRdd)
    javaRdd = encodedRdd.toJavaRDD()

    pythonRdd = RDD(javaRdd, sc, AutoBatchedSerializer(ser))

    thing = pythonRdd.collect()
    print "THIS IS THE ITEM HELD WITHIN THE PYTHON RDD:", thing

    rdd2 = pythonRdd.map(lambda s: SpatialKey(s.col+1, s.row+1))
    result = rdd2.collect()
    print "THIS IS THE RESULT OF THE PYTHON RDD BEING MAPPED OVER:", result
