Changelog
==========


0.3.0
------

New Features
^^^^^^^^^^^^^

Aggregating a Layer By Cell
****************************

It is now possible to aggregate the cells of all values that share a key
in a layer via the ``aggregate_by_cell`` method. This method is useful when
you have a layer where you want to reduce all of the values by their key.

.. code:: python3

   # A tiled layer which contains duplicate keys with different values
   # that we'd like to reduce so that there is one value per key.
   tiled_layer = ...

   # This will compute the aggregate SUM of each cell of values that share
   # a key within the layer.
   tiled_layer.aggregate_by_cell(gps.Operation.SUM)

   # Similar to the above command, only this one is finding the STANDARD_DEVIATION
   # for each cell.
   tiled_layer.aggregate_by_cell(gps.Operation.STANDARD_DEVIATION)

Unioning Layers Together
************************

Through the ``union`` method, it is now possible to union together an arbitrary number
of either ``RasterLayer``\s or ``TiledRasterLayers``.

.. code:: python3

   # Layers to be unioned together
   layers = [raster_layer_1, raster_layer_2, raster_layer_3]

   unioned_layers = gps.union(layers)

Getting Point Values From a Layer
**********************************

By using the ``get_point_values`` method, one can retrieve data points that falls
on or near a given point.

.. code:: python3

   from shapely.geometry import Point

   # The points we'd like to collect data at
   p1 = Point(0, 0)
   p2 = Point(1, 1)
   p3 = Point(10, 10)

   # The tiled layer which will be queried
   tiled_layer = ...

   tiled_layer.get_point_values([p1, p2, p3])

The above code will return a ``[(Point, [float])]`` where each
point given will be paired with all of the values it covers (one for
each band of the Tile).

It is also possible to pass in a ``dict`` to ``get_point_values``.

.. code:: python3

   labeled_points = {'p1': p1, 'p2': p2, 'p3': p3}

   tiled_layer.get_point_values(labeled_points)

This will return a ``{k: (Point, [float])}`` which is similar to
the above code only now the ``(Point, [float])`` is the value
of the key that point had in the input ``dict``.

Combining Bands of Multiple Layers
***********************************

``combine_bands`` will concatenate the bands of values that
share a key together to produce a new, single value. This new
Tile will contain all of the bands from all of the values
that shared a key from the given layers.

This method is most useful when you have multiple layers
that contain a single band from a multiband image; and you'd
like to combine them together so that all or some of the bands
are available from a single layer.


.. code:: python3

   # Three different layers that contain a single band from the
   # same scene
   band_1_layer = ...
   band_2_layer = ...
   band_3_layer = ...

   # combined_layer will have values that contain three bands: the first
   # from band_1_layer, the second from band_2_layer, and the last from
   # band_3_layer
   combined_layer = gps.combine_bands([band_1_layer, band_2_layer, band_3_layer])

Other New Features
*******************

 - `Merge method for RasterLayer and TiledRasterLayer <https://github.com/locationtech-labs/geopyspark/pull/503>`__
 - `Filter a RasterLayer or a TiledRasterLayer by time <https://github.com/locationtech-labs/geopyspark/pull/518>`__
 - `Polygonal Summary on all bands <https://github.com/locationtech-labs/geopyspark/pull/519>`__
 - `Better temporal resolution control when writing layers <https://github.com/locationtech-labs/geopyspark/pull/542>`__
 - `TiledRasterLayers can now perform the abs local operation <https://github.com/locationtech-labs/geopyspark/pull/550>`__
 - `TiledRasterLayers can now perform the ** local operation <https://github.com/locationtech-labs/geopyspark/pull/551>`__

Bug Fixes
^^^^^^^^^^

 - `LayerType creation issue <https://github.com/locationtech-labs/geopyspark/pull/494>`__
 - `tuple serializer creation fix <https://github.com/locationtech-labs/geopyspark/pull/497>`__
 - `The TMS can now read from MultibandTile catalogs <https://github.com/locationtech-labs/geopyspark/pull/508>`__
 - `tileToLayout bug <https://github.com/locationtech-labs/geopyspark/pull/525>`__
 - `additional_jar_dirs fix <https://github.com/locationtech-labs/geopyspark/pull/532>`__
 - `stitch and saveStitch now work with MultibandTiles <https://github.com/locationtech-labs/geopyspark/pull/537>`__

0.2.2
------

0.2.2 fixes the naming issue brought about in 0.2.1 where the backend jar and
the docs had the incorrect version number.


**geopyspark**

  - Fixed version numbers for docs and jar.


0.2.1
------

0.2.1 adds two major bug fixes for the ``catalog.query`` and ``geotiff.get``
functions as well as a few other minor changes/additions.


**geopyspark**

  - Updated description in ``setup.py``.

**geopyspark.geotrellis**

  - Fixed a bug in ``catalog.query`` where the query would fail if the geometry
    used for querying was in a different projection than the source layer.
  - ``partition_bytes`` can now be set in the ``geotiff.get`` function when
    reading from S3.
  - Setting ``max_tile_size`` and ``num_partitions`` in ``geotiff.get`` will now
    work when trying to read geotiffs from S3.


0.2.0
-----

The second release of GeoPySpark has brought about massive changes to the
library. Many more features have been added, and some have been taken away. The
API has also been overhauld, and code written using the 0.1.0 code will not work
with this version.

Because so much has changed over these past few months, only the most major
changes will be discussed below.


**geopyspark**

  - Removed ``GeoPyContext``.
  - Added ``geopyspark_conf`` function which is used to create a ``SparkConf`` for
    GeoPySpark.
  - Changed how the environemnt is constructed when using GeoPySpark.

**geopyspark.geotrellis**

  - A ``SparkContext`` instance is no longer needs to be passed in for any class
    or function.
  - Renamed ``RasterRDD`` and ``TiledRasterRDD`` to ``RasterLayer`` and
    ``TiledRasterLayer``.
  - Changed how ``tile_to_layout`` and ``reproject`` work.
  - Broked out ``rasterize``, ``hillshade``, ``cost_distance``, and
    ``euclidean_distance`` into their own, respective modules.
  - Added the ``Pyramid`` class to ``layer.py``.
  - Renamed ``geotiff_rdd`` to ``geotiff``.
  - Broke out the options in ``geotiff.get``.
  - Constants are now orginized by enum classes.
  - Avro is no longer used for serialization/deserialization.
  - ProtoBuf is now used for serialization/deserialization.
  - Added the ``render`` module.
  - Added the ``color`` mdoule.
  - Added the ``histogram`` moudle.

**Documentation**

  - Updated all of the docstrings to reflect the new changes.
  - All of the documentation has been updated to reflect the new chnagtes.
  - Example jupyter notebooks have been added.


0.1.0
------

The first release of GeoPySpark! After being in development for the past 6
months, it is now ready for its initial release! Since nothing has been changed
or updated per se, we'll just go over the features that will be present in
0.1.0.


**geopyspark.geotrellis**

 - Create a ``RasterRDD`` from GeoTiffs that are stored locally, on S3, or on
   HDFS.
 - Serialize Python RDDs to Scala and back.
 - Perform various tiling operations such as ``tile_to_layout``, ``cut_tiles``,
   and ``pyramid``.
 - Stitch together a ``TiledRasterRDD`` to create one ``Raster``.
 - ``rasterize`` geometries and turn them into ``RasterRDD``.
 - ``reclassify`` values of Rasters in RDDs.
 - Calculate ``cost_distance`` on a ``TiledRasterRDD``.
 - Perform local and focal operations on ``TiledRasterRDD``.
 - Read, write, and query GeoTrellis tile layers.
 - Read tiles from a layer.
 - Added ``PngRDD`` to make rendering to PNGs more efficient.
 - Added ``RDDWrapper`` to provide more functionality to the RDD classes.
 - Polygonal summary methods are now available to ``TiledRasterRDD``.
 - Euclidean distance added to ``TiledRasterRDD``.
 - Neighborhoods submodule added to make focal operations easier.

**geopyspark.command**

 - GeoPySpark can now use a script to download the jar.
   Used when installing GeoPySpark from pip.

**Documentation**

 - Added docstrings to all python classes, methods, etc.
 - Core-Concepts, rdd, geopycontext, and catalog.
 - Ingesting and creating a tile server with a greyscale raster dataset.
 - Ingesting and creating a tile server with data from Sentinel.
