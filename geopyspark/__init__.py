import os


jars = 'JARS' in os.environ
pyspark_args = 'PYSPARK_SUBMIT_ARGS' in os.environ # driver (YARN)
yarn = ('SPARK_YARN_MODE' in os.environ) and (os.environ['SPARK_YARN_MODE'] == 'true') # executor (YARN)

if not jars and not pyspark_args and not yarn:
    from geopyspark.geopyspark_utils import setup_environment
    setup_environment()
