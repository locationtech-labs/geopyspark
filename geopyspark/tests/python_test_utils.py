from os import path
from zipfile import ZipFile as zipped

import numpy as np
import os


def test_path(string):
    return "".join(["geopyspark/tests/data_files/geotiff_test_files/", string])

def check_directory():
    test_path = "geopyspark/tests/data_files/geotiff_test_files/"
    if not path.exists(test_path):
        zip_files = zipped('geopyspark/tests/data_files/geotiff_test_files.zip')
        zip_files.extractall('geopyspark/tests/data_files/')
        zip_files.close()
