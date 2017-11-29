Ingesting an Image
==================

This example shows how to ingest a grayscale image and save the results
locally. It is assumed that you have already read through the
documentation on GeoPySpark before beginning this tutorial.

Getting the Data
----------------

Before we can begin with the ingest, we must first download the data
from S3. This curl command will download a file from S3 and save it to
your ``/tmp`` direcotry. The file being downloaded comes from the
`Shuttle Radar Topography Mission
(SRTM) <https://www2.jpl.nasa.gov/srtm/index.html>`__ dataset, and
contains elevation data on the east coast of Sri Lanka.

**A side note**: Files can be retrieved directly from S3 using the
methods shown in this tutorial. However, this could not be done in this
instance due to permission requirements needed to access the file.

.. code:: python3

    curl -o /tmp/cropped.tif https://s3.amazonaws.com/geopyspark-test/example-files/cropped.tif

What is an Ingest?
------------------

Before continuing on, it would be best to briefly discuss what an ingest
actually is. When data is acquired, it may cover an arbitrary spatial
extent in an arbitrary projection. This data needs to be regularized to
some expected layout and cut into tiles. After this step, we will
possess a ``TiledRasterLayer`` that can be analyzed and saved for later
use. For more information on layers and the data they hold, see the
`layers <layers.ipynb>`__ guide.

The Code
--------

With our file downloaded we can begin the ingest.

.. code:: python3

    import geopyspark as gps

    from pyspark import SparkContext

Setting Up the SparkContext
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first thing one needs to do when using GeoPySpark is to setup
``SparkContext``. Because GeoPySpark is backed by Spark, the ``pysc`` is
needed to initialize our starting classes.

For those that are already familiar with Spark, you may already know
there are multiple ways to create a ``SparkContext``. When working with
GeoPySpark, it is advised to create this instance via ``SparkConf``.
There are numerous settings for ``SparkConf``, and some **have** to be
set a certain way in order for GeoPySpark to work. Thus,
``geopyspark_conf`` was created as way for a user to set the basic
parameters without having to worry about setting the other, required
fields.

.. code:: python3

    conf = gps.geopyspark_conf(master="local[*]", appName="ingest-example")
    pysc = SparkContext(conf=conf)

Reading in the Data
~~~~~~~~~~~~~~~~~~~

After the creation of ``pysc``, we can now read in the data. For this
example, we will be reading in a single GeoTiff that contains spatial
data. Hence, why we set the ``layer_type`` to ``LayerType.SPATIAL``.

.. code:: python3

    raster_layer = gps.geotiff.get(layer_type=gps.LayerType.SPATIAL, uri="file:///tmp/cropped.tif")

Tiling the Data
~~~~~~~~~~~~~~~

It is now time to format the data within the layer to our desired
layout. The aptly named, ``tile_to_layout``, method will cut and arrange
the rasters in the layer to the layout of our choosing. This results in
us getting a new class instance of ``TiledRasterLayer``. For this
example, we will be tiling to a ``GlobalLayout``.

With our tiled data, we might like to make a tile server from it and
show it in on a map at some point. Therefore, we have to make sure that
the tiles within the layer are in the right projection. We can do this
by setting the ``target_crs`` parameter.

.. code:: python3

    tiled_raster_layer = raster_layer.tile_to_layout(gps.GlobalLayout(), target_crs=3857)
    tiled_raster_layer

Pyramiding the Data
~~~~~~~~~~~~~~~~~~~

Now it's time to pyramid! With our reprojected data, we will create an
instance of ``Pyramid`` that contains 12 ``TiledRasterLayer``\ s. Each
one having it's own ``zoom_level`` from 11 to 0.

.. code:: python3

    pyramided_layer = tiled_raster_layer.pyramid()
    pyramided_layer.max_zoom

.. code:: python3

    pyramided_layer.levels

Saving the Pyramid Locally
~~~~~~~~~~~~~~~~~~~~~~~~~~

To save all of the ``TiledRasterLayer``\ s within ``pyramid_layer``, we
just have to loop through values of ``pyramid_layer.level`` and write
each layer locally.

.. code:: python3

    for tiled_layer in pyramided_layer.levels.values():
        gps.write(uri="file:///tmp/ingested-image", layer_name="ingested-image", tiled_raster_layer=tiled_layer)
