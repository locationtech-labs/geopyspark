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

    def __init__(self, schema_json, custom_decoder=None, custom_encoder=None):
        self._schema_json = schema_json
        self.custom_decoder = custom_decoder
        self.custom_encoder = custom_encoder

    def schema(self):
        return avro.schema.Parse(self._schema_json)

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

        if isinstance(obj, list) and isinstance(obj[0], tuple):
            tuple_datums = [self._make_datum(tup) for tup in obj]

            datum = {
                    'pairs': tuple_datums
                    }

            return datum


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
        elif isinstance(obj, list):
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
    def _make_decoder(self, name=None):

        if name is None:
            name = self.schema_name()

        def make_tile_decoder(i):
            cells = i['cells']

            if isinstance(cells, bytes):
                cells = bytearray(cells)

            # cols and rows are opposte for GeoTrellis ArrayTiles and Numpy Arrays
            arr = np.array(cells).reshape(i['rows'], i['cols'])
            tile = TileArray(arr, i['noDataValue'])

            return tile

        def make_extent_decoder(i):
            return Extent(i['xmin'], i['ymin'], i['xmax'], i['ymax'])

        def make_spatial_key_decoder(i):
            return SpatialKey(i['col'], i['row'])

        def make_spacetime_key_decoder(i):
            return SpaceTimeKey(i['col'], i['row'], i['instant'])

        if name in TILES:
            return make_tile_decoder

        elif name == EXTENT:
            return make_extent_decoder

        elif name == SPATIALKEY:
            return make_spatial_key_decoder

        elif name == SPACETIMEKEY:
            return make_spacetime_key_decoder

        else:
            raise Exception("COULDN'T FIND THE SCHEMA")

    def _make_tuple(self, i, schema=None):

        schema_1 = i['_1']
        schema_2 = i['_2']

        if schema is None:
            import json
            schema_dict = json.loads(self._schema_json)
        else:
            schema_dict = schema

        (a, b) = schema_dict['fields']

        name_1 = a['type']['name']
        if isinstance(b['type'], list):
            # The 'name' parameter does not effect the handling of
            # tiles in _make_object, so any name will do.  If type is
            # an array but this is not a tile, then undefined
            # behavior.
            if b['type'][0]['name'] in TILES:
                name_2 = b['type'][0]['name']
            else:
                name_2 = None
        else:
            name_2 = b['type']['name']

        decoder_1 = self._make_decoder(name=name_1)
        decoder_2 = self._make_decoder(name=name_2)

        result = (decoder_1(schema_1), decoder_2(schema_2))

        if schema is None:
            return [result]
        else:
            return result

    def _make_collection(self, i):

        if self.schema_name() == KEYVALUERECORD:
            import json

            schema_dict = json.loads(self._schema_json)
            fields = schema_dict['fields'][0]['type']['items']
            pairs = i['pairs']

            objs = [[self._make_tuple(x, schema=fields) for x in pairs]]

            return objs

        elif self.schema_name() == TUPLE:
            objs = self._make_tuple(i)

            return objs

        elif self.schema_name() == ARRAYMULTIBANDTILE:
            bands = i['bands']

            decoder = self._make_decoder(name='Tile')
            objs = [[decoder(x) for x in bands]]

            return objs

        else:
            decoder = self._make_decoder()
            return [decoder(i)]


    """
    Deserializes a byte array into a collection of python objects.
    """
    def loads(self, obj):
        buf = io.BytesIO(obj)
        decoder = avro.io.BinaryDecoder(buf)
        i = self.reader().read(decoder)

        python_obj_collection = self._make_collection(i)

        return python_obj_collection
