import os
from os import path
import shutil
from zipfile import ZipFile as zipped


def geotiff_test_path(file_test_path):
    root_geotiff_dir = "geopyspark/tests/data_files/geotiff_test_files/"
    result = os.path.abspath(os.path.join(root_geotiff_dir, file_test_path))

    return result

def check_directory():
    test_path = "geopyspark/tests/data_files/geotiff_test_files/"
    if os.path.exists(test_path):
        test_path_time = os.path.getmtime(test_path)
    else:
        test_path_time = 0
    zip_file = "geopyspark/tests/data_files/geotiff_test_files.zip"
    zip_file_time = os.path.getmtime(zip_file)

    if test_path_time < zip_file_time:
        shutil.rmtree(test_path, ignore_errors=True)
        zip_files = zipped(zip_file)
        zip_files.extractall('geopyspark/tests/data_files/')
        zip_files.close()
