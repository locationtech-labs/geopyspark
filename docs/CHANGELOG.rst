Changelog
==========

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
