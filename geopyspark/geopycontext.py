"""A wrapper for ``SparkContext`` that provides extra functionality for GeoPySpark."""
from geopyspark.protobufserializer import ProtoBufSerializer
from geopyspark.geopyspark_utils import check_environment
import geopyspark.geotrellis.converters

check_environment()

from pyspark import RDD, SparkContext
from pyspark.serializers import AutoBatchedSerializer

from py4j.java_gateway import java_import

class GeoPyContext(object):
    """A wrapper of ``SparkContext``.
    This wrapper provides extra functionality by providing methods that help with sending/recieving
    information to/from python.

    Args:
        pysc (pypspark.SparkContext, optional): An existing ``SparkContext``.
        **kwargs: ``GeoPyContext`` can create a ``SparkContext`` if given its constructing
            arguments.

    Note:
        If both ``pysc`` and ``kwargs`` are set the ``pysc`` will be used.

    Attributes:
        pysc (pyspark.SparkContext): The wrapped ``SparkContext``.
        sc (org.apache.spark.SparkContext): The scala ``SparkContext`` derived from the python one.

    Raises:
        TypeError: If neither a ``SparkContext`` or its constructing arguments are given.

    Examples:
        Creating ``GeoPyContext`` from an existing ``SparkContext``.

        >>> sc = SparkContext(appName="example", master="local[*]")
        >>> SparkContext
        >>> geopysc = GeoPyContext(sc)
        >>> GeoPyContext

        Creating ``GeoPyContext`` from the constructing arguments of ``SparkContext``.

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

    @staticmethod
    def map_key_input(key_type, is_boundable):
        """Gets the mapped GeoTrellis type from the `key_type`.

        Args:
            key_type (str): The type of the ``K`` in the tuple, ``(K, V)`` in the RDD.
            is_boundable (bool): Is ``K`` boundable.

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

    def create_python_rdd(self, jrdd, serializer):
        """Creates a Python RDD from a RDD from Scala.

        Args:
            jrdd (org.apache.spark.api.java.JavaRDD): The RDD that came from Scala.
            serializer (:class:`~geopyspark.AvroSerializer` or pyspark.serializers.AutoBatchedSerializer(AvroSerializer)):
                An instance of ``AvroSerializer`` that is either alone, or wrapped by ``AutoBatchedSerializer``.

        Returns:
            ``pyspark.RDD``
        """

        if isinstance(serializer, AutoBatchedSerializer):
            return RDD(jrdd, self.pysc, serializer)
        else:
            return RDD(jrdd, self.pysc, AutoBatchedSerializer(serializer))
