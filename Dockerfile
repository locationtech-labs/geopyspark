FROM quay.io/geodocker/jupyter:0

COPY geotrellis-backend-assembly-0.1.0.jar /opt/jars/
COPY geopyspark-0.1.0-py3-none-any.whl /usr/lib/python3.4/site-packages/

USER root
RUN pip3 install /usr/lib/python3.4/site-packages/geopyspark-0.1.0-py3-none-any.whl
USER jack
