from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from geopyspark.spatial_key import SpatialKey
from geopyspark.extent import Extent
from geopyspark.tile import TileArray
from io import StringIO

import io
import avro
import avro.io
import numpy as np
import array

# Constants

EXTENT = 'Extent'

TILES = ['BitArrayTile', 'ByteArrayTile', 'UByteArrayTile', 'ShortArrayTile',
        'UShortArrayTile', 'IntArrayTile', 'FloatArrayTile', 'DoubleArrayTile']

ARRAYMULTIBANDTILE = 'ArrayMultibandTile'

SPATIALKEY = 'SpatialKey'

TUPLE = 'Tuple2'

KEYVALUERECORD = 'KeyValueRecord'


class AvroSerializer(FramedSerializer):

    def __init__(self, schemaJson):
        self._schemaJson = schemaJson

    def schema(self):
        return avro.schema.Parse(self._schemaJson)

    def reader(self):
        return avro.io.DatumReader(self.schema())

    def datum_writer(self):
        return avro.io.DatumWriter(self.schema())

    def make_datum(self, obj):

        if self.schema().name in TILES:
            (r, c) = obj.shape

            if "Byte" in self.schema().name:
                values = array.array('B', obj.flatten()).tostring()
            else:
                values = obj.flatten().tolist()

            datum = {
                    'cols': c,
                    'rows': r,
                    'cells': values,
                    'noDataValue': obj.no_data_value
                    }

            return datum

        elif isinstance(obj, Extent):
            datum = {
                    'xmin': obj.xmin,
                    'xmax': obj.xmax,
                    'ymin': obj.ymin,
                    'ymax': obj.ymax
                    }

            return datum

        elif isinstance(obj, SpatialKey):
            datum = {
                    'col': obj.col,
                    'row': obj.row
                    }

            return datum

        else:
            raise Exception("COULD NOT MAKE THE DATUM")

    """
    Serialize an object into a byte array.
    When batching is used, this will be called with an array of objects.
    """
    def dumps(self, obj, schema):
        s = avro.schema.Parse(schema)
        writer = avro.io.DatumWriter(s)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        datum = self.make_datum(obj)
        writer.write(datum, encoder)

        return bytes_writer.getvalue()

    """
    Deserializes a byte array into an object.
    """
    def loads(self, obj):
        buf = io.BytesIO(obj)
        decoder = avro.io.BinaryDecoder(buf)
        i = self.reader().read(decoder)

        schema_name = self.schema().name

        if schema_name in TILES:
            # cols and rows are opposte for GeoTrellis ArrayTiles and Numpy Arrays
            arr = np.array(bytearray(i.get('cells'))).reshape(i.get('rows'), i.get('cols'))
            tile = TileArray(arr, i.get('noDataValue'))

            return [tile]

        elif schema_name == EXTENT:
            return [Extent(i.get('xmin'), i.get('ymin'), i.get('xmax'), i.get('ymax'))]

        elif schema_name == SPATIALKEY:
            return [SpatialKey(i.get('col'), i.get('row'))]

        else:
            raise Exception("COULDN'T FIND THE SCHEMA")
