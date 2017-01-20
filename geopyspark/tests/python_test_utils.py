from os import path
from zipfile import ZipFile as zipped

import numpy as np
import os


def test_path(string):
    return "geopyspark/tests/data_files/geotiff_test_files/" + string

def check_directory():
    path = "geopyspark/tests/data_files/geotiff_test_files"
    if not path.exists(path):
        zip_files = zipped(path)
        for f in zip_files:
            if f.endswith('/'):
                os.makedirs(f)
            else:
                zipped.extract(f)
