from geopyspark.geopyspark_utils import ensure_pyspark
ensure_pyspark()
from geopyspark import get_spark_context, create_python_rdd
from geopyspark.vector_pipe import Feature, Properties
from geopyspark.vector_pipe.features_collection import FeaturesCollection

from pyspark.sql import SparkSession


__all__ = ['read']


def from_orc(source):
    pysc = get_spark_context()
    session = SparkSession.builder.config(conf=pysc.getConf()).enableHiveSupport().getOrCreate()
    features = pysc._jvm.geopyspark.vectorpipe.io.OSMReader.fromORC(session._jsparkSession, source)
    return FeaturesCollection(features)

def from_dataframe(dataframe):
    pysc = get_spark_context()
    features = pysc._jvm.geopyspark.vectorpipe.io.OSMReader.fromDataFrame(dataframe._jdf)
    return FeaturesCollection(features)


