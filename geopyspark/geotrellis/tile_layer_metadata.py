from py4j.java_gateway import java_import


class TileLayerMetadata(object):

    def __init__(self, pysc):
        self.pysc = pysc

        java_import(self.pysc._gateway.jvm,
                    "geopyspark.geotrellis.spark.TileLayerMetadataWrapper")

        self._metadata_wrapper = self.pysc._gateway.jvm.TileLayerMetadataWrapper

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

        result = self._format_strings(proj_params, epsg_code, wkt_string)

        return self._metadata_wrapper.collectPythonMetadata(rdd,
                                                            schema,
                                                            extent,
                                                            tile_layout,
                                                            result)
