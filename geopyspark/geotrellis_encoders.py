from geopyspark.keys import SpatialKey, SpaceTimeKey
from geopyspark.extent import Extent
from geopyspark.tile import TileArray
from geopyspark.projected_extent import ProjectedExtent
from geopyspark.temporal_projected_extent import TemporalProjectedExtent

from functools import partial

import numpy as np
import array


class GeoTrellisEncoder(object):
    def __init__(self, custom_class=None, custom_encoder=None):

        self.custom_class = custom_class
        self.custom_encoder = custom_encoder

    @staticmethod
    def tile_encoder(obj):
        (r, c) = obj.shape

        if obj.dtype.type == np.int8 or obj.dtype.type == np.uint8:
            values = array.array('B', obj.flatten()).tostring()
        else:
            values = obj.flatten().tolist()

        if isinstance(obj, TileArray):
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

    def projected_extent_encoder(self, obj):
        datum = {
                'extent': self.extent_encoder(obj.extent),
                'epsg': obj.epsg_code
                }

        return datum

    def temporal_projected_extent_encoder(self, obj):
        datum = {
                'extent': self.extent_encoder(obj.extent),
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

    def multiband_encoder(self, obj):
        tile_datums = list(map(self.tile_encoder, obj))

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

        if self.custom_class is not None and self.custom_encoder is not None:
            if isinstance(obj, self.custom_class):
                return self.custom_encoder

        elif isinstance(obj, list) and isinstance(obj[0], tuple):
            return self.key_value_record_encoder

        elif isinstance(obj, tuple):
            return self.tuple_encoder_creator

        elif isinstance(obj, list):
            return self.multiband_encoder

        elif isinstance(obj, TileArray) or isinstance(obj, np.ndarray):
            return self.tile_encoder

        elif isinstance(obj, Extent):
            return self.extent_encoder

        elif isinstance(obj, ProjectedExtent):
            return self.projected_extent_encoder

        elif isinstance(obj, TemporalProjectedExtent):
            return self.temporal_projected_extent_encoder

        elif isinstance(obj, SpatialKey):
            return self.spatial_key_encoder

        elif isinstance(obj, SpaceTimeKey):
            return self.spacetime_key_encoder

        else:
            raise Exception('Could not find encoder for', obj)
