import sys
import os
from os import path
import argparse

from geopyspark.geopyspark_constants import JAR, CWD


JAR_URL = 'https://github.com/locationtech-labs/geopyspark/releases/download/v0.1.0-RC1/' + JAR
DEFAULT_JAR_PATH = path.join(CWD, 'jars')
CONF = path.join(CWD, 'command', 'geopyspark.conf')


parser = argparse.ArgumentParser(description='Arg Parser for GeoPySpark')
subparser = parser.add_subparsers()

set_parser = subparser.add_parser('set', help='Sets the value of the arg')
set_parser.add_argument('--install-jar',
                        '-i',
                        nargs='?',
                        dest="jar_path",
                        default=False)

read_parser = subparser.add_parser('read', help='Reads the set value of the arg')
read_parser.add_argument('--jar-path', '-jp', action="store_true", dest='path')
read_parser.add_argument('--absolute-jar-path', '-ajp', action="store_true", dest='abs_path')


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

def _parse_set(args):
    if args.jar_path:
        download_jar(args.jar_path)
    else:
        download_jar(DEFAULT_JAR_PATH)

def _parse_read(args):
    if args.path:
        jar_path = get_jar_path()
        rel_path = path.join(path.relpath(os.getcwd(), jar_path), JAR)
        print(rel_path)

    if args.abs_path:
        print(get_jar_path())

def parse_args():
    result = parser.parse_args()

    if 'set' in sys.argv[1:]:
        _parse_set(result)
    else:
        _parse_read(result)

def main():
    parse_args()
