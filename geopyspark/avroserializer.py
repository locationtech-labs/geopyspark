"""The class which serializes/deserializes values in a RDD to/from python."""
import io
from fastavro import schemaless_writer, schemaless_reader
import avro
import avro.io

from pyspark.serializers import Serializer, FramedSerializer


class AvroSerializer(FramedSerializer):
    """The serializer used by RDDs to encode/decode values to/from python.

    Args:
        schema (str): The AvroSchema of the RDD.
        decoding_method (func, optional): The decocding function for the values within the RDD.
        encoding_method (func, optional): The encocding function for the values within the RDD.

    Attributes:
        schema (str): The AvroSchema of the RDD.
        decoding_method (func, optional): The decocding function for the values within the RDD.
        encoding_method (func, optional): The encocding function for the values within the RDD.
    """

    def __init__(self,
                 schema,
                 decoding_method=None,
                 encoding_method=None):

        super().__init__()

        self.schema_string = schema

        if decoding_method:
            self.decoding_method = decoding_method
        else:
            self.decoding_method = None

        if encoding_method:
            self.encoding_method = encoding_method
        else:
            self.encoding_method = None

    @property
    def schema(self):
        """The parsed AvroSchema."""
        return avro.schema.Parse(self.schema_string)

    @property
    def schema_name(self):
        """The name of the schema."""
        return self.schema().name

    @property
    def schema_dict(self):
        """The schema values in a dict."""
        import json

        return json.loads(self.schema_string)

    @property
    def reader(self):
        """The reader function used to read values in the RDD."""
        return avro.io.DatumReader(self.schema)

    @property
    def datum_writer(self):
        """The write function used to serialize values in the RDD."""
        return avro.io.DatumWriter(self.schema)

    def _dumps(self, obj):
        bytes_writer = io.BytesIO()

        if self.encoding_method:
            datum = self.encoding_method(obj)
            schemaless_writer(bytes_writer, self.schema_dict, datum)
        else:
            schemaless_writer(bytes_writer, self.schema_dict, datum)

        return bytes_writer.getvalue()

    def dumps(self, obj):
        """Serialize an object into a byte array.

        Note:
            When batching is used, this will be called with an array of objects.

        Args:
            obj: The object to serialized into a byte array.

        Returns:
            The byte array representation of the `obj`.
        """

        if isinstance(obj, list):
            for x in obj:
                return self._dumps(x)
        else:
            return self._dumps(obj)

    def loads(self, obj):
        """Deserializes a byte array into a collection of python objects.

        Args:
            obj: The byte array representation of an object to be deserialized into the object.

        Returns:
            A list of deserialized objects.
        """

        buf = io.BytesIO(obj)
        schema_dict = schemaless_reader(buf, self.schema_dict)

        if self.decoding_method:
            return [self.decoding_method(schema_dict)]
        else:
            return [schema_dict]
