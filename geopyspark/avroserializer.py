from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
from geopyspark.keys import SpatialKey, SpaceTimeKey
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
        'UShortArrayTile', 'IntArrayTile', 'FloatArrayTile', 'DoubleArrayTile', 'Tile']

ARRAYMULTIBANDTILE = 'ArrayMultibandTile'

SPATIALKEY = 'SpatialKey'

SPACETIMEKEY = 'SpaceTimeKey'

TUPLE = 'Tuple2'

KEYVALUERECORD = 'KeyValueRecord'


class AvroSerializer(FramedSerializer):

    def __init__(self, schemaJson):
        self._schemaJson = schemaJson

    def schema(self):
        return avro.schema.Parse(self._schemaJson)

    def schema_name(self):
        return self.schema().name

    def reader(self):
        return avro.io.DatumReader(self.schema())

    def datum_writer(self):
        return avro.io.DatumWriter(self.schema())

    """
    Creates a python dictionary that will be serialized on the python side,
    and then deserialized on the scala side.
    """
    def _make_datum(self, obj):

        # TODO: Find a way to move tuples and ArrayMutlibandTiles somewhere else

        if isinstance(obj, tuple):
            (a, b) = obj

            datum_1 = self._make_datum(a)
            datum_2 = self._make_datum(b)

            datum = {
                    '_1': datum_1,
                    '_2': datum_2
                    }

            return datum

        # ArrayMultibandTiles
        if isinstance(obj, list):
            tile_datums = [self._make_datum(tile) for tile in obj]

            datum = {
                    'bands': tile_datums
                    }

            return datum

        if isinstance(obj, TileArray):
            (r, c) = obj.shape

            if obj.dtype.type == np.int8 or obj.dtype.type == np.uint8:
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

        elif isinstance(obj, SpaceTimeKey):
            datum = {
                    'col': obj.col,
                    'row': obj.row,
                    'instant': obj.instant
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
        datum = self._make_datum(obj)
        writer.write(datum, encoder)

        return bytes_writer.getvalue()

    """
    Takes the data from the byte array and turns it into the corresponding
    python object.
    """
    def _make_object(self, i, name=None):

        if name is None:
            name = self.schema_name()

        if name in TILES:
            # cols and rows are opposte for GeoTrellis ArrayTiles and Numpy Arrays
            arr = np.array(i.get('cells')).reshape(i.get('rows'), i.get('cols'))
            tile = TileArray(arr, i.get('noDataValue'))

            return tile

        elif name == EXTENT:
            return Extent(i.get('xmin'), i.get('ymin'), i.get('xmax'), i.get('ymax'))

        elif name == SPATIALKEY:
            return SpatialKey(i.get('col'), i.get('row'))

        elif name == SPACETIMEKEY:
            return [SpaceTimeKey(i['col'], i['row'], i['instant'])]

        else:
            raise Exception("COULDN'T FIND THE SCHEMA")

    """
    Deserializes a byte array into an object.
    """
    def loads(self, obj):
        buf = io.BytesIO(obj)
        decoder = avro.io.BinaryDecoder(buf)
        i = self.reader().read(decoder)

        if self.schema_name() == TUPLE:
            import json

            schema_1 = i.get('_1')
            schema_2 = i.get('_2')

            schema_dict = json.loads(self._schemaJson)
            (a, b) = schema_dict['fields']

            name_1 = a['type']['name']
            if isinstance(b['type'], list):
                name_2 = b['type'][0]['name']
            else:
                name_2 = b['type']['name']

            result = [(self._make_object(schema_1, name=name_1),
                    self._make_object(schema_2, name=name_2))]

            return result

        elif self.schema_name() == ARRAYMULTIBANDTILE:
            bands = i.get('bands')
            objs = [[self._make_object(x, name='Tile') for x in bands]]

            return objs

        else:
            return self._make_object(i)
