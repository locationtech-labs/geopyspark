from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.singleton_base import SingletonBase

import json


class TileLayerMethods(metaclass=SingletonBase):
    def __init__(self, geopysc, avroregistry=None):
        self.geopysc = geopysc
        self.avroregistry = avroregistry

        self._metadata_wrapper = self.geopysc.tile_layer_metadata_collecter

    @staticmethod
    def _format_strings(proj_params, epsg_code, wkt_string):

        if proj_params:
            return {"projParams": proj_params}

        elif epsg_code:
            if isinstance(epsg_code, int):
                epsg_code = str(epsg_code)

            return {"epsg": epsg_code}

        elif wkt_string:
            return {"wktString": wkt_string}

        else:
            return {}

    @staticmethod
    def _get_key_value_types(schema):

        key = schema['fields'][0]['type']['name']
        value = schema['fields'][1]['type'][0]['name']

        if key == "ProjectedExtent":
            key_type = "spatial"
        else:
            key_type = "spacetime"

        if value != "ArrayMultibandTile":
            value_type = "singleband"
        else:
            value_type = "multiband"

        return (key_type, value_type)

    def _convert_to_java_rdd(self, rdd, schema):
        ser = AvroSerializer(schema, self.avroregistry)
        dumped = rdd.map(lambda value: ser.dumps(value, schema))

        return dumped._to_java_object_rdd()

    def collect_metadata(self,
                         rdd,
                         extent,
                         tile_layout,
                         proj_params=None,
                         epsg_code=None,
                         wkt_string=None):

        schema = rdd.schema
        schema_json = json.loads(schema)

        result = self._format_strings(proj_params, epsg_code, wkt_string)
        types = self._get_key_value_types(schema_json)
        java_rdd = self._convert_to_java_rdd(rdd, schema)

        metadata = self._metadata_wrapper.collectPythonMetadata(types[0],
                                                                types[1],
                                                                java_rdd.rdd(),
                                                                schema,
                                                                extent,
                                                                tile_layout,
                                                                result)

        return json.loads(metadata)

    '''
    def cut_tiles(self,
                  rdd,
                  schema,
                  tile_layer_metadata,
                  resample_method=None):

        types = self._get_key_value_types(schema)
        java_rdd = self._convert_to_java_rdd(rdd, schema)

        if resample_method is None:
            resample_dict = {}
        else:
            resample_dict = {"resampleMethod": resample_method}

        result = self._tiler_wrapper.cutTiles(types[0],
                                              types[1],
                                              java_rdd.rdd(),
                                              schema,
                                              tile_layer_metadata['layout'],
                                              tile_layer_metadata['crs'],
                                              resample_dict)

        return decode_java_rdd(self.pysc, result._1(), result._2(), self.avroregistry)
    '''
