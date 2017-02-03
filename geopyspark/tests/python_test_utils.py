from os import path
from zipfile import ZipFile as zipped

import os
import sys


def add_spark_path():
    spark_home = os.environ['SPARK_HOME']
    sys.path.append(os.path.join(spark_home, 'python'))

def geotiff_test_path(file_test_path):
    root_geotiff_dir = "geopyspark/tests/data_files/geotiff_test_files/"
    result = os.path.abspath(os.path.join(root_geotiff_dir, file_test_path))

    return result

def check_directory():
    test_path = "geopyspark/tests/data_files/geotiff_test_files/"
    if not path.exists(test_path):
        zip_files = zipped('geopyspark/tests/data_files/geotiff_test_files.zip')
        zip_files.extractall('geopyspark/tests/data_files/')
        zip_files.close()
