""""This module contains functions that create RDDs from files.

There is only one function found within this module at this time, geotiff_rdd.
"""


def geotiff_rdd(geopysc,
                rdd_type,
                uri,
                options=None,
                **kwargs):

    """Creates a RDD from GeoTiffs that are located on the local file system, HDFS, or S3.

    Args:
        geopysc (GeoPyContext): The GeoPyContext being used this session.
        rdd_type (str): What the spatial type of the geotiffs are. This is
            represented by the constants: SPATIAL and SPACETIME. Note: All of the
            GeoTiffs must have the same saptial type.
        uri (str): The path to a given file/directory.
        options (dict, optional): A dictionary of different options that are used
            when creating the RDD. This defaults to the None. If None, then the
            RDD will be created using the default options for the given backend
            in GeoTrellis. Note: key values should be in camel case, as this is
            the style that is used in Scala.
        **kwargs: Option parameters can also be entered as keyword arguements.
            Note: Defining both options and keyword arguements will cause the
            keyword arguements to be ignored in favor of options.

    Returns:
        RDD: A RDD that contains tuples of dictionaries, (K, V).
            K (dict): The projected extent information of the GeoTiff.
                Which is the pyhiscal extent and the CRS of the GeoTiff.
            V (dict): The raster tile information of the GeoTiff.
                Which is the Tile data itself represented as a numpy array,
                and the no_data_value of the Tile, if any.

    Examples:
        Reading a local GeoTiff that does not have a time component with the
        default options.

        >>> geotiff_rdd(geopysc, SPATIAL, 'file://path/to/my/geotiff.tif')
        RDD

        Reading a GeoTiff from S3 that does have a time component. An option
        parameter is also passed in which specifies that each GeoTiff should
        be cut up into 256x256 tiles.

        >>> geotiff_rdd(geopysc, SPACETIME, 's3://bucket/data/september_images/', maxTileSize=256)
        RDD
    """

    key = geopysc.map_key_input(rdd_type, False)

    if kwargs and not options:
        options = kwargs

    if 's3' in uri:
        key_and_bucket_uri = uri.split("s3://")[1]
        (bucket, prefix) = key_and_bucket_uri.split("/", 1)

        if not options:
            result = geopysc.s3_geotiff_rdd.getRDD(key,
                                                   bucket,
                                                   prefix,
                                                   geopysc.sc)
        else:
            result = geopysc.s3_geotiff_rdd.getRDD(key,
                                                   bucket,
                                                   prefix,
                                                   options,
                                                   geopysc.sc)

    else:
        if not options:
            result = geopysc.hadoop_geotiff_rdd.getRDD(key,
                                                       uri,
                                                       geopysc.sc)
        else:
            result = geopysc.hadoop_geotiff_rdd.getRDD(key,
                                                       uri,
                                                       options,
                                                       geopysc.sc)

    ser = geopysc.create_tuple_serializer(result._2(), value_type="Tile")
    return geopysc.create_python_rdd(result._1(), ser)
