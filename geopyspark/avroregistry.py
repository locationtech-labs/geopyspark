from functools import partial



class AvroRegistry(object):
    __slots__ = ['decoders', 'encoders']

    def __init__(self):

        self.decoders = {
                'BitArrayTile': self.tile_decoder,
                'ByteArrayTile': self.tile_decoder,
                'UByteArrayTile': self.tile_decoder,
                'ShortArrayTile': self.tile_decoder,
                'UShortArrayTile': self.tile_decoder,
                'IntArrayTile': self.tile_decoder,
                'FloatArrayTile': self.tile_decoder,
                'DoubleArrayTile': self.tile_decoder,
                'Extent': self.extent_decoder,
                'ProjectedExtent': self.projected_extent_decoder,
                'TemporalProjectedExtent': self.temporal_projected_extent_decoder,
                'SpatialKey': self.spatial_key_decoder,
                'SpaceTimeKey': self.spacetime_key_decoder,
                'ArrayMultibandTile': self.multiband_decoder
                }

        self.encoders = {
                'TileArray': self.tile_encoder,
                'Extent': self.extent_encoder,
                'ProjectedExtent': self.projected_extent_encoder,
                'TemporalProjectedExtent': self.temporal_projected_extent_encoder,
                'SpatialKey': self.spatial_key_encoder,
                'SpaceTimeKey': self.spacetime_key_encoder,
                }

    def add_decoder(self, custom_cls, decoding_method):
        self.decoders[type(custom_cls).__name__] = decoding_method

    def add_encoder(self, custom_cls, encoding_method):
        self.encoders[type(custom_cls).__name__] = encoding_method

    # DECODERS

    @staticmethod
    def tile_decoder(i):
        from geopyspark.tile import TileArray
        import numpy as np

        cells = i['cells']

        if isinstance(cells, bytes):
            cells = bytearray(cells)

        # cols and rows are opposte for GeoTrellis ArrayTiles and Numpy Arrays
        arr = np.array(cells).reshape(i['rows'], i['cols'])

        if 'noDataValue' in i:
            tile = TileArray(arr, i['noDataValue'])
        else:
            tile = TileArray(arr, None)

        return tile

    @staticmethod
    def extent_decoder(i):
        from geopyspark.extent import Extent

        return Extent(i['xmin'], i['ymin'], i['xmax'], i['ymax'])

    @classmethod
    def projected_extent_decoder(cls, i):
        from geopyspark.projected_extent import ProjectedExtent

        extent = cls.extent_decoder(i['extent'])
        epsg = i['epsg']

        return ProjectedExtent(extent, epsg)

    @classmethod
    def temporal_projected_extent_decoder(cls, i):
        from geopyspark.temporal_projected_extent import TemporalProjectedExtent

        extent = cls.extent_decoder(i['extent'])
        epsg = i['epsg']
        instant = i['instant']

        return TemporalProjectedExtent(extent, epsg, instant)

    @staticmethod
    def spatial_key_decoder(i):
        from geopyspark.keys import SpatialKey

        return SpatialKey(i['col'], i['row'])

    @staticmethod
    def spacetime_key_decoder(i):
        from geopyspark.keys import SpaceTimeKey

        return SpaceTimeKey(i['col'], i['row'], i['instant'])

    @classmethod
    def multiband_decoder(cls, i):
        bands = i['bands']
        objs = list(map(cls.tile_decoder, bands))

        return objs

    @staticmethod
    def tuple_decoder(i, decoder_1, decoder_2):
        schema_1 = i['_1']
        schema_2 = i['_2']

        result = (decoder_1(schema_1), decoder_2(schema_2))

        return result

    def key_value_record_decoder(self, i, schema_dict):
        tuple2 = schema_dict['fields'][0]['type']['items']
        pairs = i['pairs']

        decoder = self.tuple_decoder_creator(schema_dict=tuple2)

        objs = list(map(decoder, pairs))

        return objs

    def tuple_decoder_creator(self, schema_dict):
        (a, b) = schema_dict['fields']

        name_1 = a['type']['name']
        if isinstance(b['type'], list):
            # The 'name' parameter does not effect the handling of
            # tiles in _make_object, so any name will do.  If type is
            # an array but this is not a tile, then undefined
            # behavior.
            if 'ArrayTile' in b['type'][0]['name']:
                name_2 = b['type'][0]['name']
            else:
                name_2 = None
        else:
            name_2 = b['type']['name']

        decoder_1 = self.get_decoder(name=name_1,
                schema_dict=schema_dict,)

        decoder_2 = self.get_decoder(name=name_2,
                schema_dict=schema_dict)

        return partial(self.tuple_decoder,
                decoder_1=decoder_1,
                decoder_2=decoder_2)

    def get_decoder(self, name, schema_dict):
        if name == 'KeyValueRecord':
            return partial(self.key_value_record_decoder,
                    schema_dict=schema_dict)

        elif name == 'Tuple2':
            return self.tuple_decoder_creator(schema_dict=schema_dict)

        elif name in self.decoders.keys():
            return self.decoders[name]

        else:
            raise Exception("Could not find a decoder for", name)

    # ENCODERS

    @staticmethod
    def tile_encoder(obj):
        import numpy as np
        import array

        (r, c) = obj.shape

        if obj.dtype.type == np.int8 or obj.dtype.type == np.uint8:
            values = array.array('B', obj.flatten()).tostring()
        else:
            values = obj.flatten().tolist()

        if obj.no_data_value is not None:
            datum = {
                    'cols': c,
                    'rows': r,
                    'cells': values,
                    'noDataValue': obj.no_data_value
                    }
        else:
            datum = {
                    'cols': c,
                    'rows': r,
                    'cells': values
                    }

        return datum

    @staticmethod
    def extent_encoder(obj):
        datum = {
                'xmin': obj.xmin,
                'xmax': obj.xmax,
                'ymin': obj.ymin,
                'ymax': obj.ymax
                }

        return datum

    @classmethod
    def projected_extent_encoder(cls, obj):
        datum = {
                'extent': cls.extent_encoder(obj.extent),
                'epsg': obj.epsg_code
                }

        return datum

    @classmethod
    def temporal_projected_extent_encoder(cls, obj):
        datum = {
                'extent': cls.extent_encoder(obj.extent),
                'epsg': obj.epsg_code,
                'instant': obj.instant
                }

        return datum

    @staticmethod
    def spatial_key_encoder(obj):
        datum = {
                'col': obj.col,
                'row': obj.row
                }

        return datum

    @staticmethod
    def spacetime_key_encoder(obj):
        datum = {
                'col': obj.col,
                'row': obj.row,
                'instant': obj.instant
                }

        return datum

    @classmethod
    def multiband_encoder(cls, obj):
        tile_datums = list(map(cls.tile_encoder, obj))

        datum = {
                'bands': tile_datums
                }

        return datum

    @staticmethod
    def tuple_encoder(obj, encoder_1, encoder_2):
        (a, b) = obj

        datum_1 = encoder_1(a)
        datum_2 = encoder_2(b)

        datum = {
                '_1': datum_1,
                '_2': datum_2
                }

        return datum

    def key_value_record_encoder(self, obj):
        encoder = self.tuple_encoder_creator(obj[0])

        tuple_datums = list(map(encoder, obj))

        datum = {
                'pairs': tuple_datums
                }

        return datum

    def tuple_encoder_creator(self, obj):
        (a, b) = obj

        encoder_1 = self.get_encoder(a)
        encoder_2 = self.get_encoder(b)

        return partial(self.tuple_encoder,
                encoder_1=encoder_1,
                encoder_2=encoder_2)

    def get_encoder(self, obj):
        if type(obj).__name__ in self.encoders.keys():
            return self.encoders[type(obj).__name__]

        elif isinstance(obj, list) and isinstance(obj[0], tuple):
            return self.key_value_record_encoder

        elif isinstance(obj, tuple):
            return self.tuple_encoder_creator(obj)

        elif isinstance(obj, list):
            return self.multiband_encoder

        else:
            raise Exception('Could not find encoder for', obj)
