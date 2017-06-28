import os
from os import path
import shutil
from zipfile import ZipFile as zipped

from geopyspark.geopyspark_constants import CWD


root_geotiff_dir = path.join(CWD, "tests/data_files/geotiff_test_files/")
files_path = path.relpath(root_geotiff_dir, os.getcwd())

def geotiff_test_path(file_test_path):
    return path.abspath(path.join(root_geotiff_dir, file_test_path))

def check_directory():
    test_path = path.join(CWD, "tests/data_files/geotiff_test_files/")
    if os.path.exists(test_path):
        test_path_time = os.path.getmtime(test_path)
    else:
        test_path_time = 0
    zip_file = path.join(CWD, 'tests/data_files/geotiff_test_files.zip')
    zip_file_time = os.path.getmtime(zip_file)

    if test_path_time < zip_file_time:
        shutil.rmtree(test_path, ignore_errors=True)
        zip_files = zipped(zip_file)
        zip_files.extractall(path.join(CWD, 'tests/data_files/'))
        zip_files.close()
