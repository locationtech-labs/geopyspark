from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer

class S3GeoTiffRDD(object):
    def __init__(self,
            pysc,
            custom_name=None,
            custom_decoder=None,
            custom_class=None,
            custom_encoder=None):

        self.pysc = pysc
        self.custom_name = custom_name
        self.custom_decoder = custom_decoder
        self.custom_class = custom_class
        self.custom_encoder = custom_encoder

        self._sc = self.pysc._jsc.sc()

        java_import(self.pysc._gateway.jvm,
                "geopyspark.geotrellis.io.s3.S3GeoTiffRDDWrapper")

        self._s3_wrapper = self.pysc._gateway.jvm.S3GeoTiffRDDWrapper

    def _return_rdd(self, java_rdd, schema):
        ser = AvroSerializer(schema,
                self.custom_name,
                self.custom_decoder,
                self.custom_class,
                self.custom_encoder)

        return RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser))

    def spatial(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.spatial(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.spatial(bucket, prefix, options, self._sc)

        return self._return_rdd(result._1(), result._2())

    def spatial_multiband(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.spatialMultiband(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.spatialMultiband(bucket, prefix, options, self._sc)

        return self._return_rdd(result._1(), result._2())

    def temporal(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.temporal(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.temporal(bucket, prefix, options, self._sc)

        return self._return_rdd(result._1(), result._2())

    def temporal_multiband(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.temporalMultiband(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.temporalMultiband(bucket, prefix, options, self._sc)

        return self._return_rdd(result._1(), result._2())
