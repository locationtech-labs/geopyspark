import sys
import os
import textwrap


VERSION = '0.1.0'
JAR = 'geotrellis-backend-assembly-' + VERSION + '.jar'
JAR_URL = 'https://github.com/locationtech-labs/geopyspark/releases/download/v0.1.0-RC1/' + JAR
CWD = os.path.abspath(os.path.dirname(__file__))
DEFAULT_JAR_PATH = CWD + '/jars/'


def download_jar(path=DEFAULT_JAR_PATH):
    import subprocess
    jar_location = path + JAR

    subprocess.call(['curl', '-L', JAR_URL, '-o', jar_location])

    with open(CWD + 'geopyspark.conf', 'w') as f:
        f.write(jar_location)

def get_jar_path():
    try:
        with open(CWD + 'geopyspark.conf', 'r') as f:
            return f.read()
    except:
        raise Exception("The jar path has never been set!")

def parse_args():
    if '--install-jar' in sys.argv[1:]:
        if len(sys.argv) == 2:
            download_jar()
        else:
            download_jar(sys.argv[2])
    elif '--jar-path' in sys.argv[1:]:
        print(os.path.relpath(get_jar_path(), os.getcwd()))
    elif '--absolute-jar-path' in sys.argv[1:]:
        print(get_jar_path())
    elif '--help' in sys.argv[1:]:
        print(textwrap.dedent("""
                              geopyspark

                               --install-jar [path] Downloads the jar to either the default or specified path.

                               --jar-path Gets the relative path of the jar.

                               --absolute-jar-path Gets the absolute path of the jar.

                              """))
    else:
        print('Could not parse arguements. Use --help for help.')

def main():
    parse_args()
