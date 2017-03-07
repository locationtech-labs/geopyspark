from geopyspark.geopycontext import GeoPyContext


def _map_inputs(key_type, value_type):
    if key_type == "spatial":
        key = "ProjectedExtent"
    elif key_type == "spacetime":
        key = "TemporalProjectedExtent"
    else:
        raise Exception("Could not find key type that matches", key_type)

    if value_type == "singleband":
        value = "Tile"
    elif value_type == "multiband":
        value = "MultibandTile"
    else:
        raise Exception("Could not find value type that matches", value_type)

    return (key, value)


class HadoopGeoTiffRDD(object):
    def __init__(self, geopysc):
        self.geopysc = geopysc
        self._hadoop_wrapper = self.geopysc.hadoop_geotiff_rdd

    def get_rdd(self, key_type, value_type, path, options=None):
        key, value = _map_inputs(key_type, value_type)
        if options is None:
            result = self._hadoop_wrapper.getRDD(key, value, path, self.geopysc.sc)
        else:
            result = self._hadoop_wrapper.getRDD(key, value, path, options, self.geopysc.sc)

        return self.geopysc.avro_rdd_to_python(key, value, result._1(), result._2())


class S3GeoTiffRDD(object):
    def __init__(self, geopysc):
        self.geopysc = geopysc
        self._s3_wrapper = self.geopysc.s3_geotiff_rdd

    def get_rdd(self, key_type, value_type, bucket, prefix, options=None):
        key, value = _map_inputs(key_type, value_type)
        if options is None:
            result = self._s3_wrapper.getRDD(key, value, bucket, prefix, self.geopysc.sc)
        else:
            result = self._s3_wrapper.getRDD(key, value, bucket, prefix, options, self.geopysc.sc)

        return self.geopysc.avro_rdd_to_python(key, value, result._1(), result._2())
