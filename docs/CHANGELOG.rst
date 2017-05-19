Changelog
==========

0.1.0
------

The first release of GeoPySpark! After being in development for the past 5
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

**Documentation**

 - Added docstrings to all python classes, methods, etc.
 - Core-Concepts.
 - Ingesting and creating a tile server with a greyscale data.
 - Ingesting and creating a tile server with data from Sentinel.
