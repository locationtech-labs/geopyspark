from geopyspark.geopycontext import GeoPyContext


class HadoopGeoTiffRDD(object):
    def __init__(self, geopysc):
        self.geopysc = geopysc
        self._hadoop_wrapper = self.geopysc.hadoop_geotiff_rdd

    def get_rdd(self, key_type, path, options=None):
        key = self.geopysc.map_key_input(key_type, False)

        if options is None:
            result = self._hadoop_wrapper.getRDD(key, path, self.geopysc.sc)
        else:
            result = self._hadoop_wrapper.getRDD(key, path, options, self.geopysc.sc)

        ser = self.geopysc.create_tuple_serializer(result._2(), value_type="Tile")

        return self.geopysc.create_python_rdd(result._1(), ser)


class S3GeoTiffRDD(object):
    def __init__(self, geopysc):
        self.geopysc = geopysc
        self._s3_wrapper = self.geopysc.s3_geotiff_rdd

    def get_rdd(self, key_type, bucket, prefix, options=None):
        key = self.geopysc.map_key_input(key_type, False)

        if options is None:
            result = self._s3_wrapper.getRDD(key, bucket, prefix, self.geopysc.sc)
        else:
            result = self._s3_wrapper.getRDD(key, bucket, prefix, options, self.geopysc.sc)

        ser = self.geopysc.create_tuple_serializer(result._2(), value_type="Tile")

        return self.geopysc.create_python_rdd(result._1(), ser)
