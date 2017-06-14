"""The class which serializes/deserializes values in a RDD to/from Python."""
import io
from geopyspark.geopyspark_utils import check_environment
check_environment()

from pyspark.serializers import FramedSerializer


class ProtoBufSerializer(FramedSerializer):
    """The serializer used by a RDD to encode/decode values to/from Python.

    Args:
        schema (str): The AvroSchema of the RDD.
        decoding_method (func, optional): The decocding function for the values within the RDD.
        encoding_method (func, optional): The encocding function for the values within the RDD.

    Attributes:
        schema (str): The AvroSchema of the RDD.
        decoding_method (func, optional): The decocding function for the values within the RDD.
        encoding_method (func, optional): The encocding function for the values within the RDD.
    """

    def __init__(self, decoding_method, encoding_method):

        __slots__ = [decoding_method, encoding_method]

        super().__init__()

        self.decoding_method = decoding_method
        self.encoding_method = encoding_method

    def _dumps(self, obj):
        return self.encoding_method(obj)

    def dumps(self, obj):
        """Serialize an object into a byte array.

        Note:
            When batching is used, this will be called with a list of objects.

        Args:
            obj: The object to serialized into a byte array.

        Returns:
            The byte array representation of the ``obj``.
        """

        if isinstance(obj, list):
            for x in obj:
                return self._dumps(x)
        else:
            return self._dumps(obj)

    def loads(self, obj):
        """Deserializes a byte array into a collection of Python objects.

        Args:
            obj: The byte array representation of an object to be deserialized into the object.

        Returns:
            A list of deserialized objects.
        """
        return [self.decoding_method(obj)]
