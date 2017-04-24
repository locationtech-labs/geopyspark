GeoPySpark
==========

``geopyspark`` provides Python bindings for working with geospatial data on `PySpark <http://spark.apache.org/docs/latest/api/python/pyspark.html>`_
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

How to Install
--------------

Before installing, check the above table to make sure that the above
requirements are met.

To install via ``pip`` open the terminal and run the following:

.. code::

   pip install geopyspark

If you would rather install from source, you can do so by running the following
in the terminal:

.. code::

   git clone https://github.com/locationtech-labs/geopyspark.git
   cd geopyspark
   make install

This will assemble the backend-end ``jar`` that contains the Scala code,
move it to the ``jars`` module, and then runs the ``setup.py`` script.

Make Targets
-------------

 - **install** - install ``geopyspark`` Python package locally
 - **wheel** - build python ``geopyspark`` wheel for distribution
 - **pyspark** - start PySpark shell with project jars
 - **docker-build** - build docker image for Jupyter with ``geopyspark``
