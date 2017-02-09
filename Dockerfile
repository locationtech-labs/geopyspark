FROM quay.io/geodocker/geotrellis-jupyter:latest

COPY geopyspark-backend/geotrellis/target/scala-2.11/geotrellis-backend-assembly-0.1.0.jar /opt/jars
COPY dist/geopyspark-0.1.0-py3.5.egg /usr/lib/python3.4/site-packages
