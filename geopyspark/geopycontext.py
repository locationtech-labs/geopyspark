"""A wrapper for `SparkContext` that provides extra functionality for GeoPySpark."""
from geopyspark.avroregistry import AvroRegistry
from geopyspark.avroserializer import AvroSerializer

from pyspark import RDD, SparkContext
from pyspark.serializers import AutoBatchedSerializer


class GeoPyContext(object):
    """A wrapper of `SparkContext`.
    This wrapper provides extra functionality by providing methods that help with sending/recieving
    information to/from python.

    Args:
        pysc (SparkContext, optional): An existing `SparkContext`.
        **kwargs: GeoPyContext can create a `SparkContext` if given its constructing arguments.

    Note:
        If both `pysc` and `kwargs` are set the `pysc` will be used.

    Attributes:
        pysc (SparkContext): The wrapped `SparkContext`.
        sc (org.apache.spark.SparkContext): The scala `SparkContext` derived from the python one.

    Raises:
        TypeError if neither a `SparkContext` or its constructing arguments are given.

    Examples:
        Creating `GeoPyContext` from an existing `SparkContext`.

        >>> sc = SparkContext(appName="example", master="local[*]")
        >>> SparkContext
        >>> geopysc = GeoPyContext(sc)
        >>> GeoPyContext

        Creating `GeoPyContext` from the constructing arguments of `SparkContext`.

        >>> geopysc = GeoPyContext(appName="example", master="local[*]")
        >>> GeoPyContext

    """

    def __init__(self, pysc=None, **kwargs):
        if pysc:
            self.pysc = pysc
        elif kwargs:
            self.pysc = SparkContext(**kwargs)
        else:
            raise TypeError(("Either a SparkContext or its constructing"
                             " parameters must be given,"
                             " but none were found"))

        self.sc = self.pysc._jsc.sc()
        self._jvm = self.pysc._gateway.jvm

        self.avroregistry = AvroRegistry()

    @staticmethod
    def map_key_input(key_type, is_boundable):
        """Gets the mapped GeoTrellis type from the `key_type`.

        Args:
            key_type (str): The type of the K in the tuple, (K, V) in the RDD.
            is_boundable (bool): The type of K boundable.

        Returns:
            The corresponding GeoTrellis type.
        """

        if is_boundable:
            if key_type == "spatial":
                return "SpatialKey"
            elif key_type == "spacetime":
                return "SpaceTimeKey"
            else:
                raise Exception("Could not find key type that matches", key_type)
        else:
            if key_type == "spatial":
                return "ProjectedExtent"
            elif key_type == "spacetime":
                return "TemporalProjectedExtent"
            else:
                raise Exception("Could not find key type that matches", key_type)

    def create_schema(self, key_type):
        """Creates an AvroSchema.

        Args:
            key_type (str): The type of the K in the tuple, (K, V) in the RDD.

        Returns:
            An AvroSchema for the types within the RDD.
        """

        return self._jvm.geopyspark.geotrellis.SchemaProducer.getSchema(key_type)

    def create_tuple_serializer(self, schema, key_type=None, value_type=None):
        decoder = \
                self.avroregistry.create_partial_tuple_decoder(key_type=key_type,
                                                               value_type=value_type)

        encoder = \
                self.avroregistry.create_partial_tuple_encoder(key_type=key_type,
                                                               value_type=value_type)

        return AutoBatchedSerializer(AvroSerializer(schema, decoder, encoder))

    def create_value_serializer(self, schema, value_type):
        decoder = self.avroregistry._get_decoder(value_type)
        encoder = self.avroregistry._get_encoder(value_type)

        return AvroSerializer(schema, decoder, encoder)

    def create_python_rdd(self, jrdd, serializer):
        """Creates a python RDD from a RDD from scala.

        Args:
            jrdd (org.apache.spark.api.java.JavaRDD): The RDD that came from scala.
            serializer (AvroSerializer, AutoBatchedSerializer(AvroSerializer)): An AvroSerializer
                instance, or an `AvroSerializer` insteance that is wrapped by
                `AutoBatchedSerializer`.

        Returns:
            RDD
        """

        if isinstance(serializer, AutoBatchedSerializer):
            return RDD(jrdd, self.pysc, serializer)
        else:
            return RDD(jrdd, self.pysc, AutoBatchedSerializer(serializer))
