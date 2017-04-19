#!/bin/sh

export PYSPARK_DRIVER_PYTHON="/usr/bin/python3.4"
export PYSPARK_PYTHON="/usr/bin/python3.4"
export PYTHONPATH="/usr/local/spark-2.1.0-bin-hadoop2.7/python/lib/pyspark.zip:/usr/local/spark-2.1.0-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip"
$SPARK_HOME/bin/spark-submit --master local[*] --driver-memory 8G \
			     --archives /blobs/gdal-and-friends.tar.gz,/blobs/geopyspark-and-friends.tar.gz \
			     --jars /blobs/geotrellis-backend-assembly-0.1.0.jar \
			     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
			     --conf spark.executorEnv.LD_LIBRARY_PATH=gdal-and-friends.tar.gz/lib/ \
			     --conf spark.executorEnv.PYTHONPATH=geopyspark-and-friends.tar.gz/ \
			     /tmp/server.py 'file:///tmp/geonotebook' &> /dev/null &

jupyterhub --no-ssl --Spawner.notebook_dir=/home/hadoop
