"""The installation script for GeoPySpark.
Downloads the backend jar to the designated location and keeps a reference of where it was saved
to.
"""
import sys
import os
from os import path
import argparse

from geopyspark.geopyspark_constants import JAR, CWD


JAR_URL = 'https://github.com/locationtech-labs/geopyspark/releases/download/v0.1.0-RC1/' + JAR
DEFAULT_JAR_PATH = path.join(CWD, 'jars')
CONF = path.join(CWD, 'command', 'geopyspark.conf')


parser = argparse.ArgumentParser(description='Arg Parser for GeoPySpark')
parser.add_argument('--install-jar',
                    '-i',
                    nargs='?',
                    dest="jar_path",
                    default=False)

parser.add_argument('--jar-path', '-jp', action="store_true", dest='path')
parser.add_argument('--absolute-jar-path', '-ajp', action="store_true", dest='abs_path')


def write_jar_path(jar_path):
    with open(CONF, 'w') as f:
        f.write(jar_path)

def download_jar(jar_path):
    import subprocess

    jar_location = path.join(jar_path, JAR)
    write_jar_path(jar_location)

    subprocess.call(['curl', '-L', JAR_URL, '-o', jar_location])

def get_jar_path():
    if os.path.isfile(CONF):
        with open(CONF, 'r') as f:
            return f.read()
    else:
        raise Exception("The jar path has never been set.")

def parse_args():
    result = parser.parse_args()
    remaining = sys.argv[1:]

    if '--install-jar' in remaining or '-i' in remaining:
        if result.jar_path:
            download_jar(result.jar_path)
        else:
            download_jar(DEFAULT_JAR_PATH)

    elif '--jar-path' in remaining or '-jp' in remaining:
        jar_path = get_jar_path()
        rel_path = path.join(path.relpath(jar_path, os.getcwd()))
        print(rel_path)

    else:
        print(get_jar_path())

def main():
    parse_args()
