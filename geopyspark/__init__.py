from geopyspark.geopyspark_utils import ensure_pyspark
ensure_pyspark()

import os
import glob
from pyspark import SparkConf
from pkg_resources import resource_filename

from geopyspark.geopyspark_constants import JAR

def initial_spark_conf(appName=None, master=None):
    conf = SparkConf()

    if not appName:
        raise ValueError("An appName must be provided")
    else:
        conf.setAppName(appName)

    if not master:
        conf.setMaster("local[*]")
    else:
        conf.setMaster(master)

    conf.set(key='spark.ui.enabled', value='false')
    conf.set(key='spark.serializer', value='org.apache.spark.serializer.KryoSerializer')
    conf.set(key='spark.kryo.registrator', value='geotrellis.spark.io.kryo.KryoRegistrator')

    current_location = os.path.dirname(os.path.realpath(__file__))
    cwd = os.getcwd()

    local_prefixes = [
        os.path.abspath(os.path.join(current_location, 'jars')),
        os.path.abspath(os.path.join(cwd, 'jars')),
        os.path.abspath(os.path.join(cwd, '../geopyspark/jars'))
    ]
    possible_jars = [os.path.join(prefix, '*.jar') for prefix in local_prefixes]
    configuration = os.path.join(current_location, 'command', 'geopyspark.conf')

    if os.path.isfile(configuration):
        with open(os.path.join(configuration)) as conf:
            possible_jars.append(os.path.relpath(conf.read(), cwd))

    jar = os.path.abspath(resource_filename('geopyspark.jars', JAR))
    jar_dir = os.path.dirname(jar)
    if jar_dir not in local_prefixes:
        possible_jars.append(jar)

    returned = [glob.glob(jar_files) for jar_files in possible_jars]
    jars = [jar for sublist in returned for jar in sublist]

    if len(jars) == 0:
        raise IOError("Failed to find any jars. Looked at these paths {}".format(possible_jars))

    jar_string = str(jars[0])
    conf.set(key='spark.jars', value=jar_string)

    if 'TRAVIS' in os.environ:
        conf.set(key='spark.driver.memory', value='2G')
        conf.set(key='spark.executor.memory', value='2G')
    else:
        conf.set(key='spark.driver.memory', value='8G')
        conf.set(key='spark.executor.memory', value='8G')

    return conf
