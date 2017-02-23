import os
import sys

from os import path
from pkg_resources import resource_filename


VERSION = '0.1.0'
JAR_FILE = 'geotrellis-backend-assembly-' + VERSION + '.jar'


def add_jar():
    current_location = path.dirname(path.realpath(__file__))

    local_prefixes = [
        path.join(current_location, 'jar/'),
        path.join(os.getcwd(), 'jar/')
    ]

    possible_jars = [path.join(prefix, JAR_FILE) for prefix in local_prefixes]
    possible_jars.append(path.abspath(resource_filename('geopyspark.jar',
                                                        JAR_FILE)))

    try:
        jar = [jar_path for jar_path in possible_jars if path.exists(jar_path)][0]
    except IndexError:
        raise IOError("Failed to find geotrellis-backend jar. \
                      Looked at paths {}".format(possible_jars))
    os.environ['JARS'] = jar
