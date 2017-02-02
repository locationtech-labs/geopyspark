import os

jar_path = "geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar"
if not os.path.isfile(jar_path):
    raise Exception("HEY THIS DOESN'T EXIST!!!!!!!!!!!!!!")

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {} \
        --driver-class-path {} \
        --driver-memory 4G \
        --executor-memory 4G \
        pyspark-shell".format(jar_path, jar_path)
