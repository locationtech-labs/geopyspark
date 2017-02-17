from pyspark import SparkConf, SparkContext, RDD
from pyspark.serializers import AutoBatchedSerializer
from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.avroregistry import AvroRegistry
from geopyspark.geopycontext import GeoPyContext


class GeoTiffRDD:
    def _decode_java_rdd(self, java_rdd, schema, avroregistry):
        if avroregistry is None:
            ser = AvroSerializer(schema)
        else:
            ser = AvroSerializer(schema, avroregistry)

        return RDD(java_rdd, self.geopysc.pysc, AutoBatchedSerializer(ser))


class HadoopGeoTiffRDD(GeoTiffRDD):
    def __init__(self, geopysc, avroregistry=None):
        self.geopysc = geopysc

        self.avroregistry = avroregistry

        self._hadoop_wrapper = self.geopysc.hadoop_geotiff_rdd

    def get_spatial(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.readSpatialSingleband(path, self.geopysc.sc)
        else:
            result = self._hadoop_wrapper.readSpatialSingleband(path, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)

    def get_spatial_multiband(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.readSpatialMultiband(path, self.geopysc.sc)
        else:
            result = self._hadoop_wrapper.readSpatialMultiband(path, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)

    def get_spacetime(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.readSpaceTimeSingleband(path, self.geopysc.sc)
        else:
            result = self._hadoop_wrapper.readSpaceTimeSingleband(path, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)

    def get_spacetime_multiband(self, path, options=None):
        if options is None:
            result = self._hadoop_wrapper.readSpaceTimeMultiband(path, self.geopysc.sc)
        else:
            result = self._hadoop_wrapper.readSpaceTimeMultiband(path, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)


class S3GeoTiffRDD(GeoTiffRDD):
    def __init__(self, geopysc, avroregistry=None):

        self.geopysc = geopysc
        self.avroregistry = avroregistry

        self._s3_wrapper = self.geopysc.s3_geotiff_rdd

    def get_spatial(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.readSpatialSingleband(bucket, prefix, self.geopysc.sc)
        else:
            result = self._s3_wrapper.readSpatialSingleband(bucket, prefix, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)

    def get_spatial_multiband(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.readSpatialMultiband(bucket, prefix, self.geopysc.sc)
        else:
            result = self._s3_wrapper.readSpatialMultiband(bucket, prefix, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)

    def get_spacetime(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.readSpaceTimeSingleband(bucket, prefix, self.geopysc.sc)
        else:
            result = self._s3_wrapper.readSpaceTimeSingleband(bucket, prefix, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)

    def get_spacetime_multiband(self, bucket, prefix, options=None):
        if options is None:
            result = self._s3_wrapper.readSpaceTimeMultiband(bucket, prefix, self.geopysc.sc)
        else:
            result = self._s3_wrapper.readSpaceTimeMultiband(bucket, prefix, options, self.geopysc.sc)

        return self._decode_java_rdd(result._1(), result._2(), self.avroregistry)
