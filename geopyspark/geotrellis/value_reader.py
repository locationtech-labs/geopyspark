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

        key = self.geopysc.map_key_input(self.key_type, True)
        value = self.geopysc.map_value_input(self.value_type)

        tup = self.value_reader.readTile(key,
                                         value,
                                         layer_name,
                                         zoom_level,
                                         col,
                                         row,
                                         zdt)

        if not self.ser:
            self.ser = \
                    self.geopysc.create_value_serializer(value,
                                                         tup._2())

        return self.ser.loads(tup._1())[0]


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


class FileValueReader(_ValueReader):
    def __init__(self,
                 geopysc,
                 key_type,
                 value_type,
                 path):

        super().__init__(geopysc, key_type, value_type)

        self.geopysc = geopysc
        self.key_type = key_type
        self.value_type = value_type
        self.path = path

        self.store = self.geopysc.store_factory.buildFile(self.path)
        self.value_reader = self.geopysc.value_reader_factory.buildFile(self.store)


class CassandraValueReader(_ValueReader):
    def __init__(self,
                 geopysc,
                 key_type,
                 value_type,
                 hosts,
                 username,
                 password,
                 replication_strategy,
                 replication_factor,
                 local_dc,
                 uhprd,
                 allow_remote_dcs_for_lcl,
                 attribute_key_space,
                 attribute_table):

        super().__init__(geopysc, key_type, value_type)

        self.geopysc = geopysc
        self.key_type = key_type
        self.value_type = value_type

        self.hosts = hosts
        self.username = username
        self.password = password
        self.replication_strategy = replication_strategy
        self.replication_factor = replication_factor
        self.local_dc = local_dc
        self.uhpd = uhprd
        self.allow_remote_dcs_for_lcl = allow_remote_dcs_for_lcl
        self.attribute_key_space = attribute_key_space
        self.attribute_table = attribute_table

        self.store = self.geopysc.store_factory.buildCassandra(
            self.hosts,
            self.username,
            self.password,
            self.replication_strategy,
            self.replication_factor,
            self.local_dc,
            self.uhpd,
            self.allow_remote_dcs_for_lcl,
            self.attribute_key_space,
            self.attribute_table)

        self.value_reader = self.geopysc.value_reader_factory.buildCassandra(self.store)


class HBaseValueReader(_ValueReader):
    def __init__(self,
                 geopysc,
                 key_type,
                 value_type,
                 zookeepers,
                 master,
                 client_port,
                 attribute_table):

        super().__init__(geopysc, key_type, value_type)

        self.geopysc = geopysc
        self.key_type = key_type
        self.value_type = value_type

        self.zookeepers = zookeepers
        self.master = master
        self.client_port = client_port

        self.attribute_table = attribute_table

        self.store = \
                self.geopysc.store_factory.buildHBase(self.zookeepers,
                                                      self.master,
                                                      self.client_port,
                                                      self.attribute_table)

        self.value_reader = self.geopysc.value_reader_factory.buildHBase(self.store)


class AccumuloValueReader(_ValueReader):
    def __init__(self,
                 geopysc,
                 key_type,
                 value_type,
                 zookeepers,
                 instance_name,
                 user,
                 password,
                 attribute_table):

        super().__init__(geopysc, key_type, value_type)

        self.geopysc = geopysc
        self.key_type = key_type
        self.value_type = value_type

        self.zookeepers = zookeepers
        self.instance_name = instance_name
        self.user = user
        self.password = password

        self.attribute_table = attribute_table

        self.store = \
                self.geopysc.store_factory.buildAccumulo(self.zookeepers,
                                                         self.instance_name,
                                                         self.user,
                                                         self.password,
                                                         self.attribute_table)

        self.value_reader = self.geopysc.value_reader_factory.buildAccumulo(self.store)
