from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.singleton_base import SingletonBase
from geopyspark.geopyrdd import GeoPyRDD

import json


class TileLayerMethods(metaclass=SingletonBase):
    def __init__(self, geopysc, avroregistry=None):
        self.geopysc = geopysc
        self.avroregistry = avroregistry

        self._metadata_wrapper = self.geopysc.tile_layer_metadata_collecter
        self._tiler_wrapper = self.geopysc.tile_layer_methods

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

    def _convert_to_java_rdd(self, rdd):
        schema = rdd.schema
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

        schema_json = json.loads(rdd.schema)

        result = self._format_strings(proj_params, epsg_code, wkt_string)
        types = self._get_key_value_types(schema_json)
        java_rdd = self._convert_to_java_rdd(rdd)

        metadata = self._metadata_wrapper.collectPythonMetadata(types[0],
                                                                types[1],
                                                                java_rdd.rdd(),
                                                                rdd.schema,
                                                                extent,
                                                                tile_layout,
                                                                result)

        return json.loads(metadata)

    def cut_tiles(self,
                  rdd,
                  tile_layer_metadata,
                  resample_method=None):

        schema_json = json.loads(rdd.schema)
        types = self._get_key_value_types(schema_json)
        java_rdd = self._convert_to_java_rdd(rdd)

        if resample_method is None:
            resample_dict = {}
        else:
            resample_dict = {"resampleMethod": resample_method}

        result = self._tiler_wrapper.cutTiles(types[0],
                                              types[1],
                                              java_rdd.rdd(),
                                              rdd.schema,
                                              json.dumps(tile_layer_metadata),
                                              resample_dict)

        return GeoPyRDD(result._1(),
                        self.geopysc,
                        result._2(),
                        self.avroregistry)

    def tile_to_layout(self,
                       rdd,
                       tile_layout_metadata,
                       resample_method=None):

        schema_json = json.loads(rdd.schema)
        types = self._get_key_value_types(schema_json)
        java_rdd = self._convert_to_java_rdd(rdd)

        if resample_method is None:
            resample_dict = {}
        else:
            resample_dict = {"resampleMethod": resample_method}

        result = self._tiler_wrapper.tileToLayout(types[0],
                                                  types[1],
                                                  java_rdd.rdd(),
                                                  rdd.schema,
                                                  json.dumps(tile_layer_metadata),
                                                  resample_dict)

        return GeoPyRDD(result._1(),
                        self.geopysc,
                        result._2(),
                        self.avroregistry)
