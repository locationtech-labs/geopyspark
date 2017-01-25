from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer


class HadoopGeoTiffRDD(object):
    def __init__(self,
            pysc,
            custom_name=None,
            custom_decoder=None,
            custom_class=None,
            custom_encoder=None):

        self.pysc = pysc
        self.custom_name = custom_name,
        self.custom_decoder = custom_decoder,
        self.custom_class = custom_class,
        self.custom_encoder = custom_encoder

        self._sc = self.pysc._jsc.sc()

        java_import(self.pysc._gateway.jvm,
                "geopyspark.geotrellis.io.hadoop.HadoopGeoTiffRDDWrapper")

        self._hadoop_wrapper = self.pysc._gateway.jvm.HadoopGeoTiffRDDWrapper

    def _return_rdd(self, java_rdd, schema):
        ser = AvroSerializer(schema,
                self.custom_name,
                self.custom_decoder,
                self.custom_class,
                self.custom_encoder)

        return RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser))

    def get_spatial(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.spatial(path, self._sc)
        else:
            result = self._hadoop_wrapper.spatial(path, options, self._sc)

        return self._return_rdd(result._1(), result._2())

    def get_spatial_multiband(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.spatialMultiband(path, self._sc)
        else:
            result = self._hadoop_wrapper.spatialMultiband(path, options, self._sc)

        return self._return_rdd(result._1(), result._2())

    def get_temporal(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.temporal(path, self._sc)
        else:
            result = self._hadoop_wrapper.temporal(path, options, self._sc)

        return self._return_rdd(result._1(), result._2())

    def get_temporal_multiband(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.temporalMultiband(path, self._sc)
        else:
            result = self._hadoop_wrapper.temporalMultiband(path, options, self._sc)

        return self._return_rdd(result._1(), result._2())
