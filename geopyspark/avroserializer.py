from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from spatial_key import SpatialKey
from extent import Extent

import io
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

    def make_datum(self, obj):
        if isinstance(obj, Extent):
            datum = {
                    'xmin': obj.xmin,
                    'xmax': obj.xmax,
                    'ymin': obj.ymin,
                    'ymax': obj.ymax
                    }
            return datum
        else:
            raise Exception("COULD NOT MAKE THE DATUM")

    """
    Deserializes a byte array into an object.
    """
    def dumps(self, obj, schema):
        s = avro.schema.parse(schema)
        writer = avro.io.DatumWriter(s)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        datum = self.make_datum(obj)
        writer.write(datum, encoder)

        return bytes_writer.getvalue()
        '''

        writer = StringIO.StringIO()
        encoder = avro.io.BinaryEncoder(writer)
        datum = self.make_datum(obj)
        self.datum_writer.write(datum, encoder)
        result = writer.getvalue()
        b = bytearray()
        b.extend(map(ord, result))
        return b
        '''


    """
    Serialize an object into a byte array.
    When batching is used, this will be called with an array of objects.
    """

    def loads(self, obj):
        buf = io.BytesIO(obj)
        decoder = avro.io.BinaryDecoder(buf)
        i = self.reader.read(decoder)

        if self.schema.name == EXTENT:
            return [Extent(i.get('xmin'), i.get('ymin'), i.get('xmax'), i.get('ymax'))]
        elif self.schema.name == SPATIALKEY:
            return [SpatialKey(i.get('col'), i.get('row'))]
        else:
            raise Exception("COULDN'T FIND THE SCHEMA")
