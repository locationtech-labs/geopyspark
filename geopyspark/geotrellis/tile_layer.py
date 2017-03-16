import json

from geopyspark.avroserializer import AvroSerializer


def _convert_to_java_rdd(geopysc, rdd_type, raster_rdd):
    if isinstance(raster_rdd._jrdd_deserializer.serializer, AvroSerializer):
        ser = raster_rdd._jrdd_deserializer.serializer
        dumped = raster_rdd.map(lambda value: ser.dumps(value))
        schema = raster_rdd._jrdd_deserializer.serializer.schema_string
    else:
        schema = geopysc.create_schema(rdd_type)
        ser = geopysc.create_tuple_serializer(schema, value_type="Tile")
        reserialized_rdd = raster_rdd._reserialize(ser)

        avro_ser = reserialized_rdd._jrdd_deserializer.serializer
        dumped = reserialized_rdd.map(lambda x: avro_ser.dumps(x))

    return (dumped._to_java_object_rdd(), schema)

def collect_metadata(geopysc,
                     rdd_type,
                     raster_rdd,
                     layout_extent,
                     tile_layout,
                     output_crs=None,
                     **kwargs):

    metadata_wrapper = geopysc.tile_layer_metadata_collecter
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    if output_crs:
        output_crs = output_crs
    elif kwargs:
        output_crs = kwargs
    else:
        output_crs = {}

    metadata = metadata_wrapper.collectPythonMetadata(key,
                                                      java_rdd.rdd(),
                                                      schema,
                                                      layout_extent,
                                                      tile_layout,
                                                      output_crs)

    return json.loads(metadata)


def collect_pyramid_metadata(geopysc,
                             rdd_type,
                             raster_rdd,
                             crs,
                             tile_size,
                             resolution_threshold=0.1,
                             max_zoom=12,
                             output_crs=None,
                             **kwargs):

    metadata_wrapper = geopysc.tile_layer_metadata_collecter
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    if output_crs:
        output_crs = output_crs
    elif kwargs:
        output_crs = kwargs
    else:
        output_crs = {}

    result = metadata_wrapper.collectPythonPyramidMetadata(key,
                                                           java_rdd.rdd(),
                                                           schema,
                                                           crs,
                                                           tile_size,
                                                           resolution_threshold,
                                                           max_zoom,
                                                           output_crs)

    return (result._1(), json.loads(result._2()))

def cut_tiles(geopysc,
              rdd_type,
              raster_rdd,
              tile_layer_metadata,
              resample_method=None,
              **kwargs):

    tiler_wrapper = geopysc.tile_layer_methods
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    if resample_method:
        resample_dict = {"resampleMethod": resample_method}
    elif kwargs:
        resample_dict = kwargs
    else:
        resample_dict = {}

    result = tiler_wrapper.cutTiles(key,
                                    java_rdd.rdd(),
                                    schema,
                                    json.dumps(tile_layer_metadata),
                                    resample_dict)

    ser = geopysc.create_tuple_serializer(result._2(), value_type="Tile")

    return geopysc.create_python_rdd(result._1(), ser)

def tile_to_layout(geopysc,
                   rdd_type,
                   raster_rdd,
                   tile_layer_metadata,
                   resample_method=None,
                   **kwargs):

    tiler_wrapper = geopysc.tile_layer_methods
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    if resample_method:
        resample_dict = {"resampleMethod": resample_method}
    elif kwargs:
        resample_dict = kwargs
    else:
        resample_dict = {}

    result = tiler_wrapper.tileToLayout(key,
                                        java_rdd.rdd(),
                                        schema,
                                        json.dumps(tile_layer_metadata),
                                        resample_dict)

    ser = geopysc.create_tuple_serializer(result._2(), value_type="Tile")

    return geopysc.create_python_rdd(result._1(), ser)

def merge_tiles(geopysc,
                rdd_type,
                rdd_1,
                rdd_2):

    merge_wrapper = geopysc.tile_layer_merge
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd_1, schema_1) = _convert_to_java_rdd(geopysc, key, rdd_1)
    (java_rdd_2, schema_2) = _convert_to_java_rdd(geopysc, key, rdd_2)

    result = merge_wrapper.merge(key,
                                 java_rdd_1.rdd(),
                                 schema_1,
                                 java_rdd_2.rdd(),
                                 schema_2)

    ser = geopysc.create_tuple_serializer(result._2(), value_type="Tile")

    return geopysc.create_python_rdd(result._1(), ser)

def pyramid(geopysc,
            rdd_type,
            base_raster_rdd,
            layer_metadata,
            tile_size,
            start_zoom,
            end_zoom,
            resolution_threshold=0.1,
            options=None,
            **kwargs):

    pyramider = geopysc.pyramid_builder
    key = geopysc.map_key_input(rdd_type, True)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, base_raster_rdd)

    if options:
        options = options
    elif kwargs:
        options = kwargs
    else:
        options = {}

    result = pyramider.buildPythonPyramid(key,
                                          java_rdd.rdd(),
                                          schema,
                                          json.dumps(layer_metadata),
                                          tile_size,
                                          resolution_threshold,
                                          start_zoom,
                                          end_zoom,
                                          options)

    def formatter(x):
        (rdd, schema) = (x._2()._1(), x._2()._2())
        ser = geopysc.create_tuple_serializer(schema, value_type="Tile")
        returned_rdd = geopysc.create_python_rdd(rdd, ser)

        return (x._1(), returned_rdd, json.loads(x._3()))

    return [formatter(x) for x in result]
