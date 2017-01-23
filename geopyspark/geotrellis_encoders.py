from geopyspark.keys import SpatialKey, SpaceTimeKey
from geopyspark.extent import Extent
from geopyspark.tile import TileArray
from geopyspark.projected_extent import ProjectedExtent
from geopyspark.temporal_projected_extent import TemporalProjectedExtent

from functools import partial

import numpy as np
import array


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

def extent_encoder(obj):
    datum = {
            'xmin': obj.xmin,
            'xmax': obj.xmax,
            'ymin': obj.ymin,
            'ymax': obj.ymax
            }

    return datum

def projected_extent_encoder(obj):
    datum = {
            'extent': extent_encoder(obj.extent),
            'epsg': obj.epsg_code
            }

    return datum

def temporal_projected_extent_encoder(obj):
    datum = {
            'extent': extent_encoder(obj.extent),
            'epsg': obj.epsg_code,
            'instant': obj.instant
            }

    return datum

def spatial_key_encoder(obj):
    datum = {
            'col': obj.col,
            'row': obj.row
            }

    return datum

def spacetime_key_encoder(obj):
    datum = {
            'col': obj.col,
            'row': obj.row,
            'instant': obj.instant
            }

    return datum

def multiband_encoder(obj):
    tile_datums = list(map(tile_encoder, obj))

    datum = {
            'bands': tile_datums
            }

    return datum

def tuple_encoder(obj, encoders):
    (a, b) = obj
    (encoder_1, encoder_2) = encoders

    datum_1 = encoder_1(a)
    datum_2 = encoder_2(b)

    datum = {
            '_1': datum_1,
            '_2': datum_2
            }

    return datum

def key_value_record_encoder(obj, custom_class=None, custom_encoder=None):
    encoder = tuple_encoder_creator(obj[0],
            custom_class=custom_class,
            custom_encoder=custom_encoder)

    tuple_datums = list(map(encoder, obj))

    datum = {
            'pairs': tuple_datums
            }

    return datum

def tuple_encoder_creator(obj, custom_class=None, custom_encoder=None):
    (a, b) = obj

    # TODO: FIND A BETTER WAY TO CHECK FOR CUSTOM TYPES IN TUPLES
    encoder_1 = get_encoder(a,
            custom_class=custom_class,
            custom_encoder=custom_encoder)

    encoder_2 = get_encoder(b,
            custom_class=custom_class,
            custom_encoder=custom_encoder)

    return partial(tuple_encoder, encoders=(encoder_1, encoder_2))

def get_encoder(obj, custom_class=None, custom_encoder=None):

    if custom_class is not None and custom_encoder is not None:
        if isinstance(obj, custom_class):
            return custom_encoder

    elif isinstance(obj, list) and isinstance(obj[0], tuple):
        return partial(key_value_record_encoder,
                custom_class=custom_class,
                custom_encoder=custom_encoder)

    elif isinstance(obj, tuple):
        return tuple_encoder_creator(obj,
                custom_class=custom_class,
                custom_encoder=custom_encoder)

    elif isinstance(obj, list):
        return multiband_encoder

    elif isinstance(obj, TileArray) or isinstance(obj, np.ndarray):
        return tile_encoder

    elif isinstance(obj, Extent):
        return extent_encoder

    elif isinstance(obj, ProjectedExtent):
        return projected_extent_encoder

    elif isinstance(obj, TemporalProjectedExtent):
        return temporal_projected_extent_encoder

    elif isinstance(obj, SpatialKey):
        return spatial_key_encoder

    elif isinstance(obj, SpaceTimeKey):
        return spacetime_key_encoder

    else:
        raise Exception('Could not find encoder for', obj)
