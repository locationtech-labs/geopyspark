GeoPySpark
==========

``geopyspark`` provides python bindings for working with geospatial data on `PySpark <http://spark.apache.org/docs/latest/api/python/pyspark.html>`_
It will provide interfaces into GeoTrellis and GeoMesa LocationTech frameworks.
It is currently under development, and is pre-alpha quality.

GeoPySpark Requirements
------------------------

============ ============
Requirement  Version
============ ============
Java         >=1.8.0_111
Scala        2.11.8
Python       3.3 - 3.5
Hadoop       >=2.0.1
============ ============

Make Targets
-------------

 - **isntall** - install ``geopyspark`` python package locally
 - **wheel** - build python ``geopyspark`` wheel for distribution
 - **pyspark** - start pyspark shell with project jars
 - **docker-build** - build docker image for Jupyter with ``geopyspark``
