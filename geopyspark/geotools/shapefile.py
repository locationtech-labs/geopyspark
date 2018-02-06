from geopyspark.geopyspark_utils import ensure_pyspark
ensure_pyspark()

from geopyspark import get_spark_context, create_python_rdd
from geopyspark.geotrellis.protobufserializer import ProtoBufSerializer
from geopyspark.geotrellis.constants import DEFAULT_S3_CLIENT

from geopyspark.vector_pipe import Feature

from geopyspark.geotools.geotools_protobufcodecs import feature_decoder

__all__ = ['get']


def get(uri,
        extensions=['.shp', '.SHP'],
        num_partitions=None,
        s3_client=DEFAULT_S3_CLIENT):
    """Creates an ``RDD[Feature]`` from Shapefile(s) that are located on the local file system, ``HDFS``,
    or ``S3``.

    The ``properties`` of the ``Feautre``\s in the ``RDD`` will contain the attributes of their
    respective geometry in a ``dict``. All keys and values of each ``dict`` will be ``str``\s regardless
    of how the attribute is represented in the Shapefile.

    Note:
        This feature is currently experimental and will most likely change in the coming versions of
        GPS.

    Note:
        When reading from S3, the desired files **must** be publicly readable. Otherwise, you will
        get 403 errors.

        Due to the nature of how GPS reads Shapefile(s) from S3, the ``mock`` S3 Client cannot
        currently be used.

    Args:
        uri (str or [str]): The path or list of paths to the desired Shapfile(s)/directory(ies).
        extensions ([str], optional): A list of the extensions that the Shapefile(s) have.
            These are ``.shp`` and ``.SHP`` by default.
        num_partitions (int, optional): The number of partitions Spark
            will make when the ``RDD`` is created. If ``None``, then the
            ``defaultParallelism`` will be used.
        s3_client (str, optional): Which ``S3Cleint`` to use when reading
            GeoTiffs from S3. There are currently two options: ``default`` and
            ``mock``. Defaults to :const:`~geopyspark.geotrellis.constants.DEFAULT_S3_CLIENT`.

            Note:
                ``mock`` should only be used in unit tests and debugging.

    Returns:
        ``RDD[:class:`~geopyspark.vector_pipe.Feature`]``
    """

    pysc = get_spark_context()

    num_partitions = num_partitions or pysc.defaultParallelism

    shapefile = pysc._gateway.jvm.geopyspark.geotools.shapefile.ShapefileRDD

    if isinstance(uri, (list, tuple)):
        jrdd = shapefile.get(pysc._jsc.sc(), uri, extensions, num_partitions, s3_client)
    else:
        jrdd = shapefile.get(pysc._jsc.sc(), [uri], extensions, num_partitions, s3_client)

    ser = ProtoBufSerializer(feature_decoder, None)

    return create_python_rdd(jrdd, ser)
