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

print "VERSION", sys.version


class SpatialKey(object):
    def __init__(self, col, row):
        self.col = col
        self.row = row

    def __repr__(self):
        return "SpatialKey(%d,%d)" % (self.col, self.row)


class AvroSerializer(FramedSerializer):

    def __init__(self, schema):
        self._schema = avro.schema.parse(schema)
        self._reader = avro.io.DatumReader(self._schema)

    def dumps(self, obj):
        print "DUMPS: ", obj
        writer = StringIO.StringIO()
        encoder = avro.io.BinaryEncoder(writer)
        datum_writer = avro.io.DatumWriter(self._schema)
        datum = {
            'col': obj[0].col,
            'row': obj[0].row
        }
        datum_writer.write(datum, encoder)
        return writer.getvalue()


        """
        Serialize an object into a byte array.
        When batching is used, this will be called with an array of objects.
        """

        raise NotImplementedError

    def loads(self, obj):
        print "Type:",  type(obj)
        print "OBJ:", binascii.hexlify(obj)    
        buf = io.BytesIO(obj)
        reader = avro.io.DatumReader(self._schema)
        decoder = avro.io.BinaryDecoder(buf)
        i = reader.read(decoder)
        return [SpatialKey(i.get('col'), i.get('row'))]


if __name__ == "__main__":
    schema = """{"type":"record","name":"SpatialKey","namespace":"geotrellis.spark","fields":[{"name":"col","type":"int"},{"name":"row","type":"int"}]}"""
    ser = AvroSerializer(schema)
    sc = SparkContext(master="local", appName="python-test", serializer=ser)


    java_import(sc._gateway.jvm, "geotrellis.spark.io.hadoop.*")
    java_import(sc._gateway.jvm, "geotrellis.spark.etl.*")

    # path = "/Users/eugene/tmp/HousVacScal_142_675.tif"

    # hssgr = sc._gateway.jvm.HadoopSpatialSinglebandGeoTiffRDD(path, sc._jsc)
    # crs = hssgr.fromEpsgCode(3355)

    # original_splits = hssgr.split(258, 258)
    #print ' THESE ARE THE SPLITS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1'
    #print original_splits
    

    testRdd = sc._gateway.jvm.Box.testRdd(sc._jsc.sc())
    encodedRdd = sc._gateway.jvm.Box.encodeRdd(testRdd)
    javaRdd = encodedRdd.toJavaRDD()

    pythonRdd = RDD(javaRdd, sc, AutoBatchedSerializer(ser))

    thing = pythonRdd.collect()
    print "THING:", thing

    rdd2 = pythonRdd.map(lambda s: SpatialKey(s.col+1, s.row+1))
    print "THING2:", rdd2.collect()

    # this will fail
    #sc._gateway.jvm.original_splits.map(lambda x: x)
    
    # reprojected_splits = hssgr.reproject(original_splits, crs)

    # original_value = original_splits.first()
    # reprojected_value = reprojected_splits.first()

    # print original_value._1(), reprojected_value._1()
