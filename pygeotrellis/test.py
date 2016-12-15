from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer
from py4j.java_gateway import java_import

import io
import zlib
import struct
import sys
import avro
import avro.io
import binascii

print "VERSION", sys.version

class AvroSerializer(Serializer):

    def __init__(self, schema):
        self._schema = avro.schema.parse(schema)
        self._reader = avro.io.DatumReader(self._schema)

    def dump_stream(self, iterator, stream):
        """
        Serialize an iterator of objects to the output stream.
        """
        raise NotImplementedError

    def load_stream(self, stream):
        bytes = stream.read()        
        print "BYTES: ", binascii.hexlify(bytes)
        return None
        

        # reader = avro.io.DatumReader(self._schema)
        # decoder = avro.io.BinaryDecoder(stream)
        # return reader.read(decoder).items()

    def _load_stream_without_unbatching(self, stream):
        return self.load_stream(stream)



if __name__ == "__main__":
    sc = SparkContext(master="local", appName="python-test")

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
    schema = sc._gateway.jvm.Box.keySchema()
    print sc._gateway.jvm.Box.encodeRddText(testRdd).collect()
    
    print "SCHEMA: ", schema
    pythonRdd = RDD(javaRdd, sc, AvroSerializer(schema))

    thing = pythonRdd.collect()
    print "THING:", thing


    # this will fail
    #sc._gateway.jvm.original_splits.map(lambda x: x)
    
    # reprojected_splits = hssgr.reproject(original_splits, crs)

    # original_value = original_splits.first()
    # reprojected_value = reprojected_splits.first()

    # print original_value._1(), reprojected_value._1()
