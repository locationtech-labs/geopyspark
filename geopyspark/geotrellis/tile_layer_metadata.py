from py4j.java_gateway import java_import
from geopyspark.avroserializer import AvroSerializer
from geopyspark.singleton_base import SingletonBase

import json


class TileLayerMethods(metaclass=SingletonBase):

    def __init__(self, pysc, avroregistry=None):
        self.pysc = pysc
        self.avroregistry = avroregistry

        java_import(self.pysc._gateway.jvm, "geopyspark.geotrellis.spark.TileLayerMetadataCollector")

        self._metadata_wrapper = self.pysc._gateway.jvm.TileLayerMetadataCollector

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

    def collect_metadata(self,
                         rdd,
                         schema,
                         extent,
                         tile_layout,
                         proj_params=None,
                         epsg_code=None,
                         wkt_string=None):

        schema_json = json.loads(schema)

        result = self._format_strings(proj_params, epsg_code, wkt_string)

        key = schema_json['fields'][0]['type']['name']
        value = schema_json['fields'][1]['type'][0]['name']

        if key == "ProjectedExtent":
            key_type = "spatial"
        else:
            key_type = "spacetime"

        if value != "ArrayMultibandTile":
            value_type = "singleband"
        else:
            value_type = "multiband"

        ser = AvroSerializer(schema, self.avroregistry)
        dumped = rdd.map(lambda value: ser.dumps(value, schema))
        java_rdd = dumped._to_java_object_rdd()

        return self._metadata_wrapper.collectPythonMetadata(key_type,
                                                            value_type,
                                                            java_rdd.rdd(),
                                                            schema,
                                                            extent,
                                                            tile_layout,
                                                            result)
