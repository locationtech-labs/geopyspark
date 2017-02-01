#!/bin/bash

(cd geopyspark-backend ; ./sbt "project geotrellis-backend" assembly)

docker-compose up

"PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 \
  docker-compose run spark-submit \
  --master local[*] \
  --jars geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar \
  geopyspark/tests/s3_geotiff_rdd_test.py
"
