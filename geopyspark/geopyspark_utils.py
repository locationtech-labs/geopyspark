"""Contains functions needed to setup the environment so that GeoPySpark can run."""
import glob
import sys

import os
from os import path
from pkg_resources import resource_filename

from geopyspark.geopyspark_constants import JAR


def check_environment():
    jars = 'JARS' in os.environ
    pyspark_args = 'PYSPARK_SUBMIT_ARGS' in os.environ # driver (YARN)
    yarn = ('SPARK_YARN_MODE' in os.environ) and \
            (os.environ['SPARK_YARN_MODE'] == 'true') # executor (YARN)

    if not jars and not pyspark_args and not yarn:
        setup_environment()


def add_pyspark_path():
    """Adds SPARK_HOME to environment variables.

    Raises:
        KeyError if SPARK_HOME could not be found.

    Raises:
        ValueError if py4j zip file could not be found.
    """

    try:
        pyspark_home = os.environ["SPARK_HOME"]
        sys.path.append(path.join(pyspark_home, 'python'))

    except:
        raise KeyError("Could not find SPARK_HOME")

    try:
        py4j_zip = glob.glob(path.join(pyspark_home, 'python', 'lib', 'py4j-*-src.zip'))
        sys.path.append(py4j_zip[0])
    except:
        raise ValueError("Could not find the py4j zip in", path.join(pyspark_home, 'python', 'lib'))


def setup_environment():
    """Sets up various environment variables that are needed to run GeoPySpark.

    Note:
        Specifying your own values for these environment variables will overwrite these.
    """

    add_pyspark_path()

    current_location = path.dirname(path.realpath(__file__))
    cwd = os.getcwd()

    local_prefixes = [
        path.abspath(path.join(current_location, 'jars/')),
        path.abspath(path.join(cwd, 'jars/')),
        path.abspath(path.join(cwd, '../geopyspark/jars/'))
    ]
    possible_jars = [path.join(prefix, '*.jar') for prefix in local_prefixes]
    configuration = path.join(current_location, 'command', 'geopyspark.conf')

    if path.isfile(configuration):
        with open(path.join(configuration)) as conf:
            possible_jars.append(path.relpath(current_location, conf.read()))
            possible_jars.append(path.relpath(cwd, conf.read()))

    jar = path.abspath(resource_filename('geopyspark.jars', JAR))
    jar_dir = os.path.dirname(jar)
    if jar_dir not in local_prefixes:
        possible_jars.append(jar)

    returned = [glob.glob(jar_files) for jar_files in possible_jars]
    jars = [jar for sublist in returned for jar in sublist]

    if len(jars) == 0:
        raise IOError("Failed to find any jars. Looked at these paths {}".format(possible_jars))

    jar_string = ','.join(jars)

    os.environ['JARS'] = jar_string
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    if 'TRAVIS' in os.environ:
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {} \
            --conf spark.ui.enabled=false \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.kyro.registrator=geotrellis.spark.io.kyro.KryoRegistrator \
            --driver-memory 2G \
            --executor-memory 2G \
            pyspark-shell".format(jar_string)
    else:
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {} \
            --conf spark.ui.enabled=false \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.kyro.registrator=geotrellis.spark.io.kyro.KryoRegistrator \
            --driver-memory 8G \
            --executor-memory 8G \
            pyspark-shell".format(jar_string)
