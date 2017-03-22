FROM quay.io/geodocker/jupyter:2

# COPY geotrellis-backend-assembly-0.1.0.jar /opt/jars/
# COPY geopyspark-0.1.0-py3-none-any.whl /usr/lib/python3.4/site-packages/

# RUN pip3 install /usr/lib/python3.4/site-packages/geopyspark-0.1.0-py3-none-any.whl
USER root
RUN yum install -y unzip && usermod -a -G root hadoop
USER hadoop
