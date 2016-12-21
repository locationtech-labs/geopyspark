from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer

import avro
import avro.io
import binascii
import StringIO

# Constants

EXTENT = 'Extent'

'''
TILE = ['BitArrayTile', 'ByteArrayTile', 'UByteArrayTile', 'ShortArrayTile',
        'UShortArrayTile', 'IntArrayTile', 'FloatArrayTile', 'DoubleArrayTile',
        'MultibandTile']
'''
SPATIALKEY = 'SpatialKey'


class AvroSerializer(FramedSerializer):

    def __init__(self, schema):
        self.schema = avro.schema.parse(schema)
        self.reader = avro.io.DatumReader(self.schema)
        self.datum_writer = avro.io.DatumWriter(self.schema)

    """
    Deserializes a byte array into an object.
    """
    def dumps(self, obj):
        writer = StringIO.StringIO()
        encoder = avro.io.BinaryEncoder(writer)
        datum = {
            'col': obj[0].col,
            'row': obj[0].row
        }
        self.datum_writer.write(datum, encoder)
        return writer.getvalue()


    """
    Serialize an object into a byte array.
    When batching is used, this will be called with an array of objects.
    """

    def loads(self, obj):
        buf = io.BytesIO(obj)
        decoder = avro.io.BinaryDecoder(buf)
        i = self.reader.read(decoder)
        return [SpatialKey(i.get('col'), i.get('row'))]
