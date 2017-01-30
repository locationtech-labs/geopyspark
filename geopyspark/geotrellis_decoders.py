from geopyspark.serialization_constants import *
from geopyspark.avroregistry import custom_decoders

from functools import partial


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
        tile = arr

    return tile

def extent_decoder(i):
    from geopyspark.extent import Extent

    return Extent(i['xmin'], i['ymin'], i['xmax'], i['ymax'])

def projected_extent_decoder(i):
    from geopyspark.projected_extent import ProjectedExtent

    extent = extent_decoder(i['extent'])
    epsg = i['epsg']

    return ProjectedExtent(extent, epsg)

def temporal_projected_extent_decoder( i):
    from geopyspark.temporal_projected_extent import TemporalProjectedExtent

    extent = extent_decoder(i['extent'])
    epsg = i['epsg']
    instant = i['instant']

    return TemporalProjectedExtent(extent, epsg, instant)

def spatial_key_decoder(i):
    from geopyspark.keys import SpatialKey

    return SpatialKey(i['col'], i['row'])

def spacetime_key_decoder(i):
    from geopyspark.keys import SpaceTimeKey

    return SpaceTimeKey(i['col'], i['row'], i['instant'])

def multiband_decoder(i):
    bands = i['bands']
    objs = list(map(tile_decoder, bands))

    return objs

def tuple_decoder(i, decoder_1, decoder_2):
    schema_1 = i['_1']
    schema_2 = i['_2']

    result = (decoder_1(schema_1), decoder_2(schema_2))

    return result

def key_value_record_decoder(i, schema_dict):
    tuple2 = schema_dict['fields'][0]['type']['items']
    pairs = i['pairs']

    decoder = tuple_decoder_creator(schema_dict=tuple2)

    objs = list(map(decoder, pairs))

    return objs

def tuple_decoder_creator(schema_dict):
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

    decoder_1 = get_decoder(name=name_1,
            schema_dict=schema_dict,)

    decoder_2 = get_decoder(name=name_2,
            schema_dict=schema_dict)

    return partial(tuple_decoder,
            decoder_1=decoder_1,
            decoder_2=decoder_2)

def get_decoder(name, schema_dict):

    if name in custom_decoders.keys():
        return custom_decoder[name]

    elif name == KEYVALUERECORD:
        return partial(key_value_record_decoder,
                schema_dict=schema_dict)

    elif name == TUPLE:
        return tuple_decoder_creator(schema_dict=schema_dict)

    elif name == ARRAYMULTIBANDTILE:
        return multiband_decoder

    elif name in TILES:
        return tile_decoder

    elif name == EXTENT:
        return extent_decoder

    elif name == PROJECTEDEXTENT:
        return projected_extent_decoder

    elif name == TEMPORALPROJECTEDEXTENT:
        return temporal_projected_extent_decoder

    elif name == SPATIALKEY:
        return spatial_key_decoder

    elif name == SPACETIMEKEY:
        return spacetime_key_decoder

    else:
        raise Exception("Could not find a decoder for", name)
