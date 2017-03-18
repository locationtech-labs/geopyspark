import json

from geopyspark.avroserializer import AvroSerializer
from geopyspark.geotrellis.constants import NEARESTNEIGHBOR


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
                     output_crs=None):

    """Collects the metadata of an RDD.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: SPATIAL and SPACETIME. Note: All of the
            GeoTiffs must have the same saptial type.
        raster_rdd(RDD): An RDD that contains tuples of (projection_info, tile).
            projection_info (dict): Contains the area on Earth the tile represents in addition to
                the tile's projection information. There are two different types of projection_info,
                ProjectedExtent and TemporalProjectedExtent. The first deals with data that
                only has a spatial component, while the latter has both a spatial and temporal
                attributes.

                Both ProjectedExtent and TemporalProjectedExtent share these fields:
                    extent (dict): The 'Bounding Box' of an area on Earth that is
                        contained within the GeoTiff.

                        The fields that are used to represent the layout_extent:
                            xmin (double): The bottom, left corner of the area.
                            ymin (double): The top, left corner of the area.
                            xmax (double): The bottom, right corner of the area.
                            ymax (double): The top, right corner of the area.

                    crs (str): The CRS that the rasters are projected in.

                TemporalProjectedExtent also have an additional field:
                    instant (int): The time stamp of the tile.

            tile (dict): The data of the tile.

                The fields to represent the tile:
                    data (np.ndarray): The tile data itself is represented as a 3D, numpy array.
                        Note, even if the data was originally singleband, it will be reformatted as
                        a multiband tile and read and saved as such.
                    no_data_value (optional): The no data value of the tile. Can be a range of
                        types including None.
        layout_extent (dict): The new 'Bounding Box' that the this layer will cover.
        tile_layout (dict): How the tiles are laid out over the area.

            The fields that are used to represent the tile_layout:
                tileCols (int): The number of columns of tiles that runs east to west.
                tileRows (int): The number of rows of tiles that runs north to south.
                tileCols (int): The number of pixel cols in tile that runs east to west.
                tileRows (int): The number of pixel rows in tile that runs north to south.
        output_crs (str, optional): The CRS that the metadata should be
            represented in. Must be in well-known name format. If None, then the GeoTiffs'
            original CRS will be used.

    Returns:
        dict: The dictionary representation of the RDD's metadata.
            The fields that are used to represent the metadata:
                cellType (str): The value type of every cell within the rasters.
                layoutDefinition (dict): Defines the raster layout of the rasters.

                The fields that are used to represent the layoutDefinition:
                    extent (dict): The area covered by the layout tiles.
                    tileLayout (dict): The tile layout of the rasters.
                extent (dict): The extent that covers the tiles.
                crs (str): The CRS that the rasters are projected in.
                bounds (dict): Represents the positions of the tile layer tiles within a gird.
                    These positions are represented by keys. There are two different types of keys,
                    SpatialKeys and SpaceTimeKeys. SpatialKeys are for data that only have a
                    spatial component while SpaceTimeKeys are for data with both spatial and
                    temporal components.

                    Both SpatialKeys and SpaceTimeKeys share these fields:
                        The fields that are used to represent the bounds:
                            minKey (dict): Represents where the tile layer begins in the gird.
                            maxKey (dict): Represents where the tile layer ends in the gird.

                        The fields that are used to represent the minKey and maxKey:
                            col (int): The column number of the grid, runs east to west.
                            row (int): The row number of the grid, runs north to south.

                    SpaceTimeKeys also have an additional field:
                        instant (int): The time stamp of the tile.
    """

    metadata_wrapper = geopysc.tile_layer_metadata_collecter
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    if output_crs:
        output_crs = output_crs
    else:
        output_crs = ""

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
                             output_crs=None):

    """Collects the metadata of an RDD as well as the zoom level of the RDD.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: SPATIAL and SPACETIME. Note: All of the
            GeoTiffs must have the same saptial type.
        crs (str): The CRS that the tiles within the RDD are in.
            The CRS must be in the well-known name format.
        tile_size (int): The size of the each tile in the RDD in terms of
            pixels. Thus, if each tile within a RDD is 256x256 then the
            tile_size will be 256. Note: All tiles within the RDD must be of the
            same size.
        resolution_threshold (double, optional): The percentage difference between
            the cell size and zoom level as well as the resolution difference between
            a given zoom level, and the next that is tolerated to snap to the lower-resolution
            zoom level. If not specified, the default value is: 0.1.
        max_zoom (int, optional): The max level this tile layer can go to.
            If not specified, the default max_zoom is: 12.
        output_crs (str, optional): The CRS that the metadata should be
            represented in. Must be in well-known name format. If None, then the GeoTiffs'
            original CRS will be used.

    Returns:
        tup: A tuple that contains the zoom level and the metadata for the tile layer.
            int: The zoom level of the tile layer.
            dict: The dictionary representation of the RDD's metadata.
                The fields that are used to represent the metadata:
                    cellType (str): The value type of every cell within the rasters.
                    layoutDefinition (dict): Defines the raster layout of the rasters.

                    The fields that are used to represent the layoutDefinition:
                        extent (dict): The area covered by the layout tiles.
                        tileLayout (dict): The tile layout of the rasters.
                        b
                        b
                    extent (dict): The extent that covers the tiles.
                    crs (str): The CRS that the rasters are projected in.
                    bounds (dict): Represents the positions of the tile layer tiles within a gird.

                        The fields that are used to represent the bounds:
                            minKey (dict): Represents where the tile layer begins in the gird.
                            maxKey (dict): Represents where the tile layer ends in the gird.

                            The fields that are used to represent the minKey and maxKey:
                                col (int): The column number of the grid, runs east to west.
                                row (int): The row number of the grid, runs north to south.
    """

    metadata_wrapper = geopysc.tile_layer_metadata_collecter
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    if output_crs:
        output_crs = output_crs
    else:
        output_crs = ""

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
              resample_method=NEARESTNEIGHBOR):

    """Cuts the tiles within a RDD to a given tile layout.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: SPATIAL and SPACETIME. Note: All of the
            GeoTiffs must have the same saptial type.
        tile_layer_metadata (dict): The metadata for this tile layer. This provides
            the layout definition that the tiles will be cut to.

            The fields that are used to represent the metadata:
                cellType (str): The value type of every cell within the rasters.
                layoutDefinition (dict): Defines the raster layout of the rasters.

                The fields that are used to represent the layoutDefinition:
                    extent (dict): The area covered by the layout tiles.
                    tileLayout (dict): The tile layout of the rasters.
                extent (dict): The extent that covers the tiles.
                crs (str): The CRS that the rasters are projected in.
                bounds (dict): Represents the positions of the tile layer tiles within a gird.

                    The fields that are used to represent the bounds:
                        minKey (dict): Represents where the tile layer begins in the gird.
                        maxKey (dict): Represents where the tile layer ends in the gird.

                        The fields that are used to represent the minKey and maxKey:
                            col (int): The column number of the grid, runs east to west.
                            row (int): The row number of the grid, runs north to south.
        resample_method (str, optional): The resample method used when creating the resulting
            tiles. If None, the default method is: NEARESTNEIGHBOR.

            The list of supported methods:
                NEARESTNEIGHBOR
                BILINEAR
                CUBICCONVOLUTION
                CUBICSPLINE
                LANCZOS
                AVERAGE
                MODE
                MEDIAN
                MAX
                MIN

    Returns:
        RDD: A RDD that contains tuples of dictionaries, (key, tile).
            key (dict): The index of the tile within the layer. There are two different types
                of keys, SpatialKeys and SpaceTimeKeys. SpatialKeys deal with data that have just
                a spatial component, whereas SpaceTimeKeys are for data with both a spatial and
                time component.

                Both SpatialKeys and SpaceTimeKeys share these fields:
                    col (int): The column number of the grid, runs east to west.
                    row (int): The row number of the grid, runs north to south.

                SpaceTimeKeys also have an additional field:
                    instant (int): The time stamp of the tile.
            tile (dict): The data of the tile.

                The fields to represent the tile:
                    data (np.ndarray): The tile data itself is represented as a 3D, numpy array.
                        Note, even if the data was originally singleband, it will be reformatted as
                        a multiband tile and read and saved as such.
                    no_data_value (optional): The no data value of the tile. Can be a range of
                        types including None.

    """
    tiler_wrapper = geopysc.tile_layer_methods
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    result = tiler_wrapper.cutTiles(key,
                                    java_rdd.rdd(),
                                    schema,
                                    json.dumps(tile_layer_metadata),
                                    resample_method)

    ser = geopysc.create_tuple_serializer(result._2(), value_type="Tile")

    return geopysc.create_python_rdd(result._1(), ser)

def tile_to_layout(geopysc,
                   rdd_type,
                   raster_rdd,
                   tile_layer_metadata,
                   resample_method=NEARESTNEIGHBOR):

    """Resamples tiles to a new resolution.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: SPATIAL and SPACETIME. Note: All of the
            GeoTiffs must have the same saptial type.
        tile_layer_metadata (dict): The metadata for this tile layer. This provides
            the information needed to resample the old tiles and create new ones.

            The fields that are used to represent the metadata:
                cellType (str): The value type of every cell within the rasters.
                layoutDefinition (dict): Defines the raster layout of the rasters.

                The fields that are used to represent the layoutDefinition:
                    extent (dict): The area covered by the layout tiles.
                    tileLayout (dict): The tile layout of the rasters.
                extent (dict): The extent that covers the tiles.
                crs (str): The CRS that the rasters are projected in.
                bounds (dict): Represents the positions of the tile layer tiles within a gird.

                    The fields that are used to represent the bounds:
                        minKey (dict): Represents where the tile layer begins in the gird.
                        maxKey (dict): Represents where the tile layer ends in the gird.

                        The fields that are used to represent the minKey and maxKey:
                            col (int): The column number of the grid, runs east to west.
                            row (int): The row number of the grid, runs north to south.

    Returns:
        RDD: A RDD that contains tuples of dictionaries, (key, tile).
            key (dict): The index of the tile within the layer. There are two different types
                of keys, SpatialKeys and SpaceTimeKeys. SpatialKeys deal with data that have just
                a spatial component, whereas SpaceTimeKeys are for data with both a spatial and
                time component.

                Both SpatialKeys and SpaceTimeKeys share these fields:
                    col (int): The column number of the grid, runs east to west.
                    row (int): The row number of the grid, runs north to south.

                SpaceTimeKeys also have an additional field:
                    instant (int): The time stamp of the tile.
            tile (dict): The data of the tile.

                The fields to represent the tile:
                    data (np.ndarray): The tile data itself is represented as a 3D, numpy array.
                        Note, even if the data was originally singleband, it will be reformatted as
                        a multiband tile and read and saved as such.
                    no_data_value (optional): The no data value of the tile. Can be a range of
                        types including None.
        resample_method (str, optional): The resample method used when creating the resulting
            tiles. If None, the default method is: NEARESTNEIGHBOR.

            The list of supported methods:
                NEARESTNEIGHBOR
                BILINEAR
                CUBICCONVOLUTION
                CUBICSPLINE
                LANCZOS
                AVERAGE
                MODE
                MEDIAN
                MAX
                MIN

    """
    tiler_wrapper = geopysc.tile_layer_methods
    key = geopysc.map_key_input(rdd_type, False)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, raster_rdd)

    result = tiler_wrapper.tileToLayout(key,
                                        java_rdd.rdd(),
                                        schema,
                                        json.dumps(tile_layer_metadata),
                                        resample_method)

    ser = geopysc.create_tuple_serializer(result._2(), value_type="Tile")

    return geopysc.create_python_rdd(result._1(), ser)

def merge_tiles(geopysc,
                rdd_type,
                rdd_1,
                rdd_2):

    """Merges the tiles of two RDDs.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: SPATIAL and SPACETIME. Note: All of the
            GeoTiffs must have the same saptial type.

    Returns:
        RDD: A RDD that contains tuples of dictionaries, (key, tile).
            key (dict): The index of the tile within the layer. There are two different types
                of keys, SpatialKeys and SpaceTimeKeys. SpatialKeys deal with data that have just
                a spatial component, whereas SpaceTimeKeys are for data with both a spatial and
                time component.

                Both SpatialKeys and SpaceTimeKeys share these fields:
                    col (int): The column number of the grid, runs east to west.
                    row (int): The row number of the grid, runs north to south.

                SpaceTimeKeys also have an additional field:
                    instant (int): The time stamp of the tile.
            tile (dict): The data of the tile.

                The fields to represent the tile:
                    data (np.ndarray): The tile data itself is represented as a 3D, numpy array.
                        Note, even if the data was originally singleband, it will be reformatted as
                        a multiband tile and read and saved as such.
                    no_data_value (optional): The no data value of the tile. Can be a range of
                        types including None.

    """
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
            resample_method=NEARESTNEIGHBOR):

    pyramider = geopysc.pyramid_builder
    key = geopysc.map_key_input(rdd_type, True)

    (java_rdd, schema) = _convert_to_java_rdd(geopysc, key, base_raster_rdd)

    result = pyramider.buildPythonPyramid(key,
                                          java_rdd.rdd(),
                                          schema,
                                          json.dumps(layer_metadata),
                                          tile_size,
                                          resolution_threshold,
                                          start_zoom,
                                          end_zoom,
                                          resample_method)

    def formatter(x):
        (rdd, schema) = (x._2()._1(), x._2()._2())
        ser = geopysc.create_tuple_serializer(schema, value_type="Tile")
        returned_rdd = geopysc.create_python_rdd(rdd, ser)

        return (x._1(), returned_rdd, json.loads(x._3()))

    return [formatter(x) for x in result]
