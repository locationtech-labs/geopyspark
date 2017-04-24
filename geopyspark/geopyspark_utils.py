"""Contains functions needed to setup the environment so that GeoPySpark can run."""
import glob
import sys

import os
from os import path
from pkg_resources import resource_filename


VERSION = '0.1.0'
JAR_FILE = 'geotrellis-backend-assembly-' + VERSION + '.jar'


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

    local_prefixes = [
        path.abspath(path.join(current_location, 'jars/')),
        path.abspath(path.join(os.getcwd(), 'jars/')),
        path.abspath(path.join(os.getcwd(), '../geopyspark/jars/'))
    ]

    possible_jars = [path.join(prefix, '*.jar') for prefix in local_prefixes]
    jar = path.abspath(resource_filename('geopyspark.jars', JAR_FILE))
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
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {} \
            --conf spark.ui.enabled=false \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --driver-memory 8G \
            --executor-memory 8G \
            pyspark-shell".format(jar_string)
