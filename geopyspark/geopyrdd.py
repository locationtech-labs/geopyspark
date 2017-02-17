from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer
from geopyspark.avroserializer import AvroSerializer


class GeoPyRDD(RDD):
    def __init__(self,
                 jrdd,
                 geopysc,
                 schema,
                 avroregistry):

        self.geopysc = geopysc
        self.schema = schema
        self.avroregistry = avroregistry
        self._ser = AvroSerializer(self.schema, self.avroregistry)

        super().__init__(jrdd, self.geopysc.pysc, AutoBatchedSerializer(self._ser))
