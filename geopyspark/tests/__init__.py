import os

from geopyspark.geopyspark_utils import add_jar


add_jar()

jar_path = os.environ['JARS']

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {} \
        --driver-class-path {} \
        --driver-memory 4G \
        --executor-memory 4G \
        pyspark-shell".format(jar_path, jar_path)
