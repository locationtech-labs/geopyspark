"""Contains the various encoding/decoding methods to bring values to/from python from scala."""
import array
from bitstring import BitArray
from functools import partial
import numpy as np
from geopyspark.geopyspark_utils import check_environment
check_environment()


class AvroRegistry(object):
    """Holds the encoding/decoding methods needed to bring scala RDDs to/from python."""

    # DECODERS

    @staticmethod
    def _tile_decoder(schema_dict):
        cells = schema_dict['cells']

        # cols and rows are opposite for GeoTrellis ArrayTiles and Numpy Arrays
        cols = schema_dict['rows']
        rows = schema_dict['cols']

        if isinstance(cells, bytes) and cols * rows == len(cells):
            cells = bytearray(cells)
        elif isinstance(cells, bytes) and cols * rows != len(cells):
            cells = bytearray(BitArray(cells))

        arr = np.array(cells).reshape(cols, rows)

        return arr

    @classmethod
    def tile_decoder(cls, schema_dict):
        """Decodes a TILE into python.

        Args:
            schema_dict (dict): The dict representation of the AvroSchema.

        Returns:
            Tile (dict)
        """

        if 'bands' not in schema_dict:
            arr = [cls._tile_decoder(schema_dict)]
            no_data = schema_dict.get('noDataValue')
            tile = np.array(arr)
        else:
            bands = schema_dict['bands']
            arrs = [cls._tile_decoder(band) for band in bands]
            no_data = bands[0].get('noDataValue')
            tile = np.array(arrs)

        return {'data': tile, 'no_data_value': no_data}

    @staticmethod
    def tuple_decoder(schema_dict, key_decoder=None, value_decoder=None):
        """Decodes a tuple into python.

        Args:
            schema_dict (dict): The dict representation of the AvroSchema.
            key_decoder (func, optional): The decoding function of the key.
            value_decoder (func, optional): The decoding function fo the value.

        Returns:
            tuple
        """

        schema_1 = schema_dict['_1']
        schema_2 = schema_dict['_2']

        if key_decoder and value_decoder:
            return (key_decoder(schema_1), value_decoder(schema_2))
        elif key_decoder:
            return (key_decoder(schema_1), schema_2)
        elif value_decoder:
            return (schema_1, value_decoder(schema_2))
        else:
            return (schema_1, schema_2)

    @classmethod
    def _get_decoder(cls, name):
        if name == "Tile":
            return cls.tile_decoder
        elif name == 'Projected':
            pass
        else:
            raise Exception("Could not find value type that matches", name)

    @classmethod
    def create_partial_tuple_decoder(cls, key_type=None, value_type=None):
        """Creates a partial, tuple decoder function.

        Args:
            key_type (str, optional): The type of the key in the tuple.
            value_type (str, optional): The type of the value in the tuple.

        Returns:
            A partial tuple_decoder function that requires a `schema_dict` to execute.
        """

        if key_type:
            key_decoder = cls._get_decoder(key_type)
        else:
            key_decoder = None

        if value_type:
            value_decoder = cls._get_decoder(value_type)
        else:
            value_decoder = None

        return partial(cls.tuple_decoder,
                       key_decoder=key_decoder,
                       value_decoder=value_decoder)

    # ENCODERS

    @staticmethod
    def _tile_encoder(obj):
        arr = obj['data']

        (rows, cols) = arr.shape

        if arr.dtype.name == 'int8' or arr.dtype.name == 'uint8':
            values = array.array('B', arr.flatten()).tostring()
        else:
            values = arr.flatten().tolist()

        if obj['no_data_value'] is not None:
            datum = {
                'cols': cols,
                'rows': rows,
                'cells': values,
                'noDataValue': obj['no_data_value']
            }

        else:
            datum = {
                'cols': cols,
                'rows': rows,
                'cells': values
            }

        return datum

    @classmethod
    def tile_encoder(cls, obj):
        """Encodes a TILE to send to scala..

        Args:
            obj (dict): The dict representation of `TILE`.

        Returns:
            avro_schema_dict (dict)
        """
        if obj['data'].ndim == 2:
            obj['data'] = np.expand_dims(obj['data'], 0)

        band_count = obj['data'].shape[0]

        def create_dict(index):
            return {'data': obj['data'][index, :, :], 'no_data_value': obj['no_data_value']}

        tile_datums = [cls._tile_encoder(create_dict(x)) for x in range(band_count)]

        return {'bands': tile_datums}

    @classmethod
    def projected_extent_encoder(cls, obj):
        if not isinstance(obj['extent'], dict):
            obj['extent'] = obj['extent']._asdict()

        if obj.get('epsg'):
            obj['proj4'] = 'null'
        else:
            obj['epsg'] = 'null'

        return obj

    @staticmethod
    def tuple_encoder(obj, key_encoder=None, value_encoder=None):
        """Encodes a tuple to send to scala..

        Args:
            obj (tuple): The tuple to be encoded.
            key_encoder (func, optional): The encoding function of the key.
            value_encoder (func, optional): The encoding function fo the value.

        Returns:
            avro_schema_dict (dict)
        """
        (value_1, value_2) = obj

        if key_encoder and value_encoder:
            datum_1 = key_encoder(value_1)
            datum_2 = value_encoder(value_2)
        elif key_encoder:
            datum_1 = key_encoder(value_1)
            datum_2 = value_2
        elif value_encoder:
            datum_1 = value_1
            datum_2 = value_encoder(value_2)
        else:
            datum_1 = value_1
            datum_2 = value_2

        return {'_1': datum_1, '_2': datum_2}

    @classmethod
    def create_partial_tuple_encoder(cls, key_type=None, value_type=None):
        """Creates a partial, tuple encoder function.

        Args:
            key_type (str, optional): The type of the key in the tuple.
            value_type (str, optional): The type of the value in the tuple.

        Returns:
            A partial tuple_encoder function that requires a `obj` to execute.
        """
        if key_type:
            key_encoder = cls._get_encoder(key_type)
        else:
            key_encoder = None

        if value_type:
            value_encoder = cls._get_encoder(value_type)
        else:
            value_encoder = None

        return partial(cls.tuple_encoder,
                       key_encoder=key_encoder,
                       value_encoder=value_encoder)

    @classmethod
    def _get_encoder(cls, name):
        if name == "Tile":
            return cls.tile_encoder
        elif name == "Projected":
            return cls.projected_extent_encoder
        else:
            raise Exception("Could not find value type that matches", name)
