GeoPySpark
***********
.. image:: https://travis-ci.org/locationtech-labs/geopyspark.svg?branch=master
   :target: https://travis-ci.org/locationtech-labs/geopyspark

.. image:: https://readthedocs.org/projects/geopyspark/badge/?version=latest
   :target: https://geopyspark.readthedocs.io/en/latest/?badge=latest

``GeoPySpark`` provides Python bindings for working with geospatial data using `PySpark <http://spark.apache.org/docs/latest/api/python/pyspark.html>`_
It will provide interfaces into GeoTrellis and GeoMesa LocationTech frameworks.
It is currently under development, and has just entered alpha.

Currently, only functionality from GeoTrellis has been supported. GeoMesa
LocationTech frameworks will be added at a later date.

Contact and Support
--------------------

If you need help, have questions, or like to talk to the developers (let us
know what you're working on!) you contact us at:

 * `Gitter <https://gitter.im/geotrellis/geotrellis>`_
 * `Mailing list <https://locationtech.org/mailman/listinfo/geotrellis-user>`_

As you may have noticed from the above links, those are links to the GeoTrellis
gitter channel and mailing list. This is because this project is currently an
offshoot of GeoTrellis, and we will be using their mailing list and gitter
channel as a means of contact. However, we will form our own if there is
a need for it.

Setup
------

GeoPySpark Requirements
^^^^^^^^^^^^^^^^^^^^^^^^

============ ============
Requirement  Version
============ ============
Java         >=1.8
Scala        2.11.8
Python       3.3 - 3.5
Hadoop       >=2.0.1
============ ============

Java 8 and Scala 2.11 are needed for GeoPySpark to work; as they are required
by GeoTrellis. In addition, Spark needs to be installed and configured with the
environment variable, ``SPARK_HOME`` set.

You can test to see if Spark is installed properly by running the following in the
terminal:

.. code:: console

   > echo $SPARK_HOME
   /usr/local/bin/spark

If the return is a path leading to your Spark folder, then it means that Spark
has been configured correctly.

How to Install
^^^^^^^^^^^^^^^

Before installing, check the above table to make sure that the
requirements are met.

To install via ``pip`` open the terminal and run the following:

.. code:: console

   pip install geopyspark
   geopyspark --install-jar [path/to/install/jar]

Where the first command installs the python code from PyPi and the second
downloads the backend, jar file. If no path is given when downloading the jar,
then it will be downloaded to wherever GeoPySpark was installed at.

What's With That Weird Pip Install?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"What's with that weird pip install?", you maybe asking yourself. The reason
for its unusualness is due to how GeoPySpark functions. Because this library
is a python binding for a Scala project, we need to be able to access the
Scala backend. To do this, we plug into PySpark which acts as a bridge between
Python and Scala. However, in order to achieve this the Scala code needs to be
assembled into a jar file. This poses a problem due to its size (117.7 MB at
v0.1.0-RC!). To get around the size constraints of PyPi, we thus utilized this
method of distribution where the jar must be downloaded in a serperate command
when using ``pip install``.

Note:
  Installing from source does not require the seperate download of the jar.

If you would rather install from source, you can do so by running the following
in the terminal:

.. code:: console

   git clone https://github.com/locationtech-labs/geopyspark.git
   cd geopyspark
   make install

This will assemble the backend-end ``jar`` that contains the Scala code,
move it to the ``jars`` module, and then runs the ``setup.py`` script.

Note:
  If you have somehow altered the global behavior of ``sbt`` this install may
  not work correctly.

Make Targets
^^^^^^^^^^^^

 - **install** - install ``GeoPySpark`` python package locally
 - **wheel** - build python ``GeoPySpark`` wheel for distribution
 - **pyspark** - start pyspark shell with project jars
 - **docker-build** - build docker image for Jupyter with ``GeoPySpark``
 - **clean** - remove the wheel, the backend jar file, and clean the
   ``geotrellis-backend`` directory
 - **cleaner** - the same as **clean**, but also erase all ``.pyc``
   files and delete binary artifacts in the ``docker`` directory

Docker Container
^^^^^^^^^^^^^^^^

To build the docker container, type the following in a terminal:

.. code:: console

   make docker-build

If you encounter problems, typing ``make cleaner`` before typing
``make docker-build`` could help.

To run the container, type:

.. code:: console

   docker run -it --rm -p 8000:8000 quay.io/geodocker/jupyter-geopyspark:3

Contributing
------------

Any kind of feedback and contributions to GeoPySpark is always welcomed.
A CLA is required for contribution, see `Contributing <docs/contributing.rst>`_ for more
information.
