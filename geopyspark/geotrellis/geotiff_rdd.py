from geopyspark.geopycontext import GeoPyContext


def geotiff_rdd(geopysc,
                rdd_type,
                uri,
                options=None,
                **kwargs):

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
