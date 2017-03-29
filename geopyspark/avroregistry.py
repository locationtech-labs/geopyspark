import array
from functools import partial
import numpy as np


class AvroRegistry(object):
    # DECODERS

    @staticmethod
    def _tile_decoder(schema_dict):
        cells = schema_dict['cells']

        if isinstance(cells, bytes):
            cells = bytearray(cells)

        # cols and rows are opposite for GeoTrellis ArrayTiles and Numpy Arrays
        arr = np.array(cells).reshape(schema_dict['rows'], schema_dict['cols'])

        return arr

    @classmethod
    def tile_decoder(cls, schema_dict):
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
    def get_decoder(cls, name):
        if name == "Tile":
            return cls.tile_decoder
        else:
            raise Exception("Could not find value type that matches", name)

    @classmethod
    def create_partial_tuple_decoder(cls, key_type=None, value_type=None):
        if key_type:
            key_decoder = cls.get_decoder(key_type)
        else:
            key_decoder = None

        if value_type:
            value_decoder = cls.get_decoder(value_type)
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
        if obj['data'].ndim == 2:
            obj['data'] = np.expand_dims(obj['data'], 0)

        band_count = obj['data'].shape[0]

        def create_dict(index):
            return {'data': obj['data'][index, :, :], 'no_data_value': obj['no_data_value']}

        tile_datums = [cls._tile_encoder(create_dict(x)) for x in range(band_count)]

        return {'bands': tile_datums}

    @staticmethod
    def tuple_encoder(obj, key_encoder=None, value_encoder=None):
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
        if key_type:
            key_encoder = cls.get_encoder(key_type)
        else:
            key_encoder = None

        if value_type:
            value_encoder = cls.get_encoder(value_type)
        else:
            value_encoder = None

        return partial(cls.tuple_encoder,
                       key_encoder=key_encoder,
                       value_encoder=value_encoder)

    @classmethod
    def get_encoder(cls, name):
        if name == "Tile":
            return cls.tile_encoder
        else:
            raise Exception("Could not find value type that matches", name)
