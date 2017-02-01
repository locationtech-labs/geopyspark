from os import path
from zipfile import ZipFile as zipped

import numpy as np
import os
import subprocess
import logging
import sys



def add_spark_path():
    jar_path = "geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar"
    if not os.path.isfile(jar_path):
        raise Exception("HEY THIS DOESN'T EXIST!!!!!!!!!!!!!!")
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {} \
            --driver-class-path {} \
            pyspark-shell".format(jar_path, jar_path)
    spark_home = os.environ['SPARK_HOME']
    sys.path.append(os.path.join(spark_home, 'python'))

def tst_path(string):
    return "".join(["geopyspark/tests/data_files/geotiff_test_files/", string])

def check_directory():
    test_path = "geopyspark/tests/data_files/geotiff_test_files/"
    if not path.exists(test_path):
        zip_files = zipped('geopyspark/tests/data_files/geotiff_test_files.zip')
        zip_files.extractall('geopyspark/tests/data_files/')
        zip_files.close()
