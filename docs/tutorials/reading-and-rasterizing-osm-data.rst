Reading and Rasterizing Open Street Map Data
---------------------------------------------


This tutorial shows how to read in Open Street Map (OSM) data, and then
rasterize it using GeoPySpark.

**Note**: This guide is aimed at users who are already familiar with GeoPySpark.


Getting the Data
=================

To start, let's first grab an `orc file <https://orc.apache.org/>`__,
which a special file type that is optimized for Hadoop operations.
The following command will use ``curl`` to download the file from
``S3`` and move it to the ``/tmp`` directory.

.. code::

  curl -o /tmp/boyertown.orc https://s3.amazonaws.com/geopyspark-test/example-files/boyertown.orc

**A side note**: Files can be retrieved directly from S3. However, this could
not be done in this instance due to permission requirements needed to access
the file.


Reading in the Data
====================

Now that we have our data, we can now read it in and begin to work with. We
will first begin by reading it in.

.. code:: python3

   import geopyspark as gps
   from pyspark import SparkContext

   conf = gps.geopyspark_conf(appName="osm-rasterize-example", master="local[*]")
   pysc = SparkContext(conf=conf)

   features = gps.osm_reader.from_orc("/tmp/boyertown.orc")

The above code sets up a ``SparkContext`` and then reads in the
``boyertown.orc`` file as ``features``, which is an instance
of ``FeaturesCollection``.

When OSM data is read into GeoPySpark, each OSM Element is turned into either
a single or multiple different geometries. With each of these geometries
retaining the metadata of the derived OSM Element. These geometry metadata
pairs are refered to as a ``Feature``. Internally, these features are
grouped together by the type of geometry they contain. When accessing
features from a ``FeaturesCollection``, it is done by geometry.

Selecting the Features We Want
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For this example, we're interested in rasterizing the ``Line``\s and
``Polygon``\s from the OSM data, so we will select those ``Feature``\s
from the ``features``. The following code will create a Python ``RDD``
of ``Feature``\s that contains all ``Line`` geometries (``lines``), and a
Python ``RDD`` that contains all ``Polygon`` geometries (``polygons``).

.. code:: python3

   lines = features.get_line_features_rdd()
   polygons = features.get_polygon_features_rdd()

Looking at the Tags of the Features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we rasterize the ``Polygon`` ``Feature``\s we would like to have some
``Polygon``\s with different values than others. In this case, we'd like
for schools to have a different value than all of the other polygons.
However, we are unsure if any schools were contained within the original data,
and we'd like to see if any are there. One method we could use to determine
if there are schools is to look at the tags of the ``Polygon`` ``Feature``\s.
The following code will show all of the unique tags for all of the ``Polygon``\s
in the collection.

.. code:: python3

   features.get_polygon_tags()

Which has the following output:


Assigning Values to Geometries
===============================

Now that we have our geometries setup, it's now time assign them values. The
reason we need to so is because when vector becomes a raster, its cells need
to have some kind of value. When rasterizing geometries, each shape will be
assigned a single value, and all cells that intersect that shape will have
that value. In addition to value of the actual cells, there's another propertry
that we will want to give each geometry. That property being ``Z-Index``.

The ``Z-Index`` of a geometry determines what value a cell will be if more than
one geometry intersects it. With a higher ``Z-Index`` taking priority over
a lower one. This is important as there may be cases where multiple geometires
are present at a single cell, but that cell can only contain one value.

For this example, we are going to want to


For example, there's a park that's a ``Polygon`` and inside the park are
benches represented as ``Point``\s. The actual cell values between the park
and the benches are different, and we'd like to make sure that the values
of the benches are present in the resulting raster. Therefore, we assign
the benches a ``Z-Index`` of 2 and the park a ``Z-Index`` of 1.
