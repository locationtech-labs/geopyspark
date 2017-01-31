from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer


class GeoTiffRDD:
    def set_custom_class(self, custom_class):
        self.custom_class = custom_class

    def _decode_java_rdd(self, java_rdd, schema):
        if self.custom_class is None:
            ser = AvroSerializer(schema)
        else:
            from geopyspark.geotrellis_decoders import GeoTrellisDecoder
            from geopyspark.geotrellis_encoders import GeoTrellisEncoder

            geotrellis_decoder = GeoTrellisDecoder(self.custom_class.name,
                    self.custom_class.custom_decoder)

            geotrellis_encoder = GeoTrellisEncoder(self.custom_class,
                    self.custom_class.custom_encoder)

            ser = AvroSerializer(schema, geotrellis_decoder, geotrellis_encoder)

        return RDD(java_rdd, self.pysc, AutoBatchedSerializer(ser))


class HadoopGeoTiffRDD(GeoTiffRDD):
    def __init__(self, pysc):

        self.pysc = pysc
        self._sc = self.pysc._jsc.sc()

        java_import(self.pysc._gateway.jvm,
                "geopyspark.geotrellis.io.hadoop.HadoopGeoTiffRDDWrapper")

        self._hadoop_wrapper = self.pysc._gateway.jvm.HadoopGeoTiffRDDWrapper

        self.custom_class = None

    def get_spatial(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.spatial(path, self._sc)
        else:
            result = self._hadoop_wrapper.spatial(path, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())

    def get_spatial_multiband(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.spatialMultiband(path, self._sc)
        else:
            result = self._hadoop_wrapper.spatialMultiband(path, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())

    def get_temporal(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.temporal(path, self._sc)
        else:
            result = self._hadoop_wrapper.temporal(path, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())

    def get_temporal_multiband(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.temporalMultiband(path, self._sc)
        else:
            result = self._hadoop_wrapper.temporalMultiband(path, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())


class S3GeoTiffRDD(GeoTiffRDD):
    def __init__(self, pysc):

        self.pysc = pysc
        self._sc = self.pysc._jsc.sc()

        java_import(self.pysc._gateway.jvm,
                "geopyspark.geotrellis.io.s3.S3GeoTiffRDDWrapper")

        self._s3_wrapper = self.pysc._gateway.jvm.S3GeoTiffRDDWrapper

        self.custom_class = None

    def get_spatial(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.spatial(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.spatial(bucket, prefix, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())

    def get_spatial_multiband(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.spatialMultiband(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.spatialMultiband(bucket, prefix, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())

    def get_temporal(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.temporal(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.temporal(bucket, prefix, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())

    def get_temporal_multiband(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.temporalMultiband(bucket, prefix, self._sc)
        else:
            result = self._s3_wrapper.temporalMultiband(bucket, prefix, options, self._sc)

        return self._decode_java_rdd(result._1(), result._2())
