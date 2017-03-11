from geopyspark.geopycontext import GeoPyContext


class _ValueReader(object):
    def __init__(self, geopysc, key_type, value_type):
        self.geopysc = geopysc
        self.key_type = key_type
        self.value_type = value_type
        self.value_reader = None
        self.ser = None

    def read(self,
             layer_name,
             zoom_level,
             col,
             row,
             zdt=None):

        if not zdt:
            zdt = ""

        tup = self.value_reader.readTile(self.key_type,
                                     self.value_type,
                                     layer_name,
                                     zoom_level,
                                     col,
                                     row,
                                     zdt)

        if not self.ser:
            self.ser = self.geopysc.create_value_serializer(self.value_type, tup._2())

        return self.ser.loads(tup._1())


class HadoopValueReader(_ValueReader):
    def __init__(self,
                 geopysc,
                 key_type,
                 value_type,
                 uri):

        super().__init__(geopysc, key_type, value_type)

        self.geopysc = geopysc
        self.key_type = key_type
        self.value_type = value_type
        self.uri = uri

        self.store = self.geopysc.store_factory.buildHadoop(self.uri)
        self.value_reader = self.geopysc.value_reader_factory.buildHadoop(self.store)


class S3ValueReader(_ValueReader):
    def __init__(self,
                 geopysc,
                 key_type,
                 value_type,
                 bucket,
                 prefix):

        super().__init__(geopysc, key_type, value_type)

        self.geopysc = geopysc
        self.key_type = key_type
        self.value_type = value_type
        self.bucket = bucket
        self.prefix = prefix

        self.store = self.geopysc.store_factory.buildS3(self.bucket, self.prefix)
        self.value_reader = self.geopysc.value_reader_factory.buildS3(self.store)
