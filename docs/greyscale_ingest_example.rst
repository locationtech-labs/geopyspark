.. _greyscale_ingest_example:

Ingesting a Grayscale Image Using GeoPySpark
********************************************

This example shows how to ingest a grayscale image from S3 and save the
results locally. The data is stored on a bucket that is open to everyone to
read, so feel free to run this example yourself.

The Code
========

Here's the code to run the ingest. One of the biggest benefits of using
GeoPySpark is how much simpler it is to run an ingest than if you were
using GeoTrellis. Whereas GeoTrellis needs various configuration files run an
ingest, GeoPySpark can perform the same task in just a single script.

.. code-block:: python

  from geopyspark.geopycontext import GeoPyContext
  from geopyspark.geotrellis.constants import SPATIAL, ZOOM
  from geopyspark.geotrellis.catalog import write
  from geopyspark.geotrellis.geotiff_rdd import get

  geopysc = GeoPyContext(appName="python-S3-ingest", master="local[*]")

  # Read the GeoTiff from S3
  rdd = get(geopysc, SPATIAL, "s3://geopyspark-test/example-files/cropped.tif")

  metadata = rdd.collect_metadata()

  # tile the rdd to the layout defined in the metadata
  laid_out = rdd.tile_to_layout(metadata)

  # reproject the tiled rasters using a ZoomedLayoutScheme
  reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)

  # pyramid the TiledRasterRDD to create 12 new TiledRasterRDDs
  # one for each zoom level
  pyramided = reprojected.pyramid(start_zoom=12, end_zoom=1)

  # Save each TiledRasterRDDs locally
  for tiled in pyramided:
      write("file:///tmp/geopyspark-catalog", "geopyspark-ingest", tiled)


Running the Code
-----------------

Running the code is simple, and you have two different ways of doing it.

The first is to copy and paste the code into a console like, iPython, and then
running it.

The second is to place this code in a Python file and then saving it. To run it
from the file, go to the directory the file is in and run this command

.. code-block:: none

  python3 file.py

Just replace ``file.py`` with whatever name you decided to call the file.

.. _break_down:

Breaking Down the Code
=======================

Now that the code has been written let's go through it step-by-step to see
what's actually going on.

The Imports
-----------

.. code-block:: python

 from geopyspark.geopycontext import GeoPyContext
 from geopyspark.geotrellis.constants import SPATIAL, ZOOM
 from geopyspark.geotrellis.catalog import write
 from geopyspark.geotrellis.geotiff_rdd import get

Pretty straight forward, though there is one thing that needs to be mentioned.
:const:`~geopyspark.geotrellis.constants.ZOOM` represents a
``ZoomedLayoutScheme``. While the exact meaning behind this won't be discussed
here (you can read more about it here (put a link here) or from the
`actual GeoTrellis docs <https://github.com/locationtech/geotrellis/blob/39e93fdbdf92d594154b82b788a9a9f7deda7dc2/docs/guide/etl.rst#layout-scheme>`_)
it is important to know that **if you wish to pyramid your data, it must be
tiled via a ZoomedLayoutScheme before the pyramiding takes place**.


Reading in the Data
--------------------

.. code-block:: python

 geopysc = GeoPyContext(appName="python-S3-ingest", master="local[*]")

 # Read the GeoTiff from S3
 rdd = get(geopysc, SPATIAL, "s3://geopyspark-test/example-files/cropped.tif")

Before doing anything when using GeoPySpark, it's best to create a
:class:`~geopysaprk.GeoPyContext` instance. This acts as a wrapper for
``SparkContext``, and provides some useful, behind-the-scenes methods for other
GeoPySpark functions.

After the creation of ``geopysc`` we can now read the data from S3. For this
example, we will be reading a single GeoTiff that contains only spatial data
(hence :const:`~geopyspark.geotrellis.SPATIAL`). This will create an instance
of :class:`~geopyspark.geotrellis.rdd.RasterRDD` which will allow us to start
working with our data.


Collecting the Metadata
------------------------

.. code-block:: python

 metadata = rdd.collect_metadata()

Before we can begin formatting the data to our desired layout, we must first
collect the :ref:`metadata` of the entire RDD. The metadata itself will contain
the :ref:`tile_layout` that the data will be formatted to. There are various
ways to collect the metadata depending on how you want the layout to look
(see :meth:`~geopyspark.geotrellis.rdd.RasterRDD.collect_metadata`), but for
this example, we will just go with the default parameters.


Tiling the Data
----------------

.. code-block:: python

 # tile the rdd to the layout defined in the metadata
 laid_out = rdd.tile_to_layout(metadata)

 # reproject the tiled rasters using a ZoomedLayoutScheme
 reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)

With the metadata collected, it is now time to format the data within the
RDD to our desired layout. The aptly named, :meth:`~geopyspark.geotrellis.rdd.RasterRDD.tile_to_layout`,
method will cut and arrange the rasters in the RDD to the layout within the
metadata; giving us a new class instance of :class:`~geopyspark.geotrellis.rdd.TiledRasterRDD`.

Having this new class will allow us to perform the final steps of our ingest.
While the tiles are now in the correct layout, their CRS is not what we want.
It would be great if we could make a tile server from our ingested data, but to
do that we'll have to change the projection.
:meth:`~geopysaprk.geotrellis.rdd.TiledRasterRDD.reproject` will be able to
help with this. This is also where ``ZOOM`` comes into play since it's at this
point where we need to format our data to have a ``ZoomedLayoutScheme``. Thus,
we select Web Mercator as our new CRS and we now have a new ``TiledRasterRDD``
that is in the correct projection and layout.


Pyramiding the Data
--------------------

.. code-block:: python

 # pyramid the TiledRasterRDD to create 12 new TiledRasterRDD
 # one for each zoom level
 pyramided = reprojected.pyramid(start_zoom=12, end_zoom=1)

Now it's time to pyramid! Using our reprojected data, we can create 12 new
instances of ``TiledRasterRDD``. Each instance represents the data within the
RDD at a specific zoom level. **Note**: The ``start_zoom`` is always the larger
number when pyramiding.


Saving the Ingest Locally
--------------------------

.. code-block:: python

 # Save each TiledRasterRDD locally
 for tiled in pyramided:
     write("file:///tmp/python-catalog", "python-ingest", tiled)

All that's left to do now is to save it. Since ``pyramided`` is just a list of
``TiledRasterRDD``, we can just loop through it and save each element one at a
time.
