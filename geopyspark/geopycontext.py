from pyspark import SparkContext
from py4j.java_gateway import java_import


class GeoPyContext(object):
    def __init__(self, pysc):
        self.pysc = pysc
        self.sc = self.pysc._jsc.sc()
        self._jvm = self.pysc._gateway.jvm

    @staticmethod
    def construct(*args, **kwargs):
        GeoPyContext(SparkContext(*args, **kwargs))

    @property
    def hadoop_geotiff_rdd(self):
        return self._jvm.geopyspark.geotrellis.io.hadoop.HadoopGeoTiffRDDWrapper

    @property
    def s3_geotiff_rdd(self):
        return self._jvm.geopyspark.geotrellis.io.s3.S3GeoTiffRDDWrapper

    @property
    def store_factory(self):
        return self._jvm.geopyspark.geotrellis.io.AttributeStoreFactory

    @property
    def reader_factory(self):
        return self._jvm.geopyspark.geotrellis.io.LayerReaderFactory

    @property
    def writer_factory(self):
        return self._jvm.geopyspark.geotrellis.io.LayerWriterFactory
