.. _core_concepts:

Core Concepts
**************

Dealing with GeoTrellis Types
=============================

Because GeoPySpark is a binding of an existing project, GeoTrellis, some
terminology and data representations have carried over. This section seeks
to explain this jargon in addition to describing how GeoTrellis types are
represented in GeoPySpark.

You may notice as read through this section that camel case is used instead of
python's more traditional naming convention for some values. This is because
scala uses this style of naming, and when it recieves data from python it
expects the value names to be in camel case.

.. _raster:

Raster
------

GeoPySpark differs in how it represents rasters from other geo-spatial python
libraries like rasterio. In GeoPySpark, they are represented as a ``dict``.

The fields used to represent rasters:
 - **no_data_value**: The value that represents no data in raster. This cen be
      represented by a variety of types depending on the value type of the
      raster.
 - **data** (nd.array): The raster data itself. It is contained within a numpy
      array.

**Note**: All rasters in GeoPySpark are represented as having multiple bands,
even if the origin raster just contained one.

.. _tile_layout:

TileLayout
----------

Describes the grid in which the rasters within a RDD should be laid out.
In GeoPySpark, this is represented as a ``dict``.

The fields used to reprsent ``TileLayout``:
 - **layoutCols** (int): The number of columns of rasters that runs
       east to west.
 - **layoutRows** (int): The number of rows of rasters that runs north to south.
 - **tileCols** (int): The number of columns of pixels in each raster that runs
       east to west.
 - **tileRows** (int): The number of rows of pixels in each raster that runs
       north to south.

Example::

   tile_layout = {'layoutCols': 1, 'layoutRows': 1, 'tileCols': 256, 'tileRows': 256}

.. _extent:

Extent
------

The "bounding box" or geographic region of an area on Earth a raster represents.
In GeoPySpark, this is represented as a ``dict``.

The fields used to represent ``Extent``:
 - **xmin** (double): The minimum x coordinate.
 - **ymin** (double): The minimum y coordinate.
 - **xmax** (double): The maximum x coordinate.
 - **ymax** (double): The maximum y coordinate.

Example::

   extent = {'xmin': 0.0, 'ymin': 1.0, 'xmax': 2.0, 'ymax': 3.0}

ProjectedExtent
---------------

Describes both the area on Earth a raster represents in addition to its CRS.
In GeoPySpark, this is represented as a ``dict``.

The fields used to represent ``ProjectedExtent``:
 - **extent** (Extent): The area the raster represents.
 - **epsg** (int, optional): The EPSG code of the CRS.
 - **proj4** (str, optional): The Proj.4 string representation of the CRS.

Example::

   extent = {'xmin': 0.0, 'ymin': 1.0, 'xmax': 2.0, 'ymax': 3.0}

   // using epsg
   epsg_code = 3857
   projected_extent = {'extent': extent, 'epsg': epsg}

   // using proj4
   proj4 = "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs "
   projected_extent = {'extent': extent, 'proj4': proj4}


**Note**: Either `epsg` or `proj4` must be definied.

TemporalProjectedExtent
-----------------------

Describes the area on Earth the raster represents, its CRS, and the time the
data was collected. In GeoPySpark, this is represented as a ``dict``.

The fields used to represent ``TemporalProjectedExtent``.
 - **extent** (Extent): The area the raster represents.
 - **epsg** (int, optional): The EPSG code of the CRS.
 - **proj4** (str, optional): The Proj.4 string representation of the CRS.
 - **instance** (int): The time stamp of the raster.

Example::

   extent = {'xmin': 0.0, 'ymin': 1.0, 'xmax': 2.0, 'ymax': 3.0}

   epsg_code = 3857
   instance = 1.0
   projected_extent = {'extent': extent, 'epsg': epsg, 'instance': instance}

**Note**: Either `epsg` or `proj4` must be definied.

SpatialKey
----------

Represents the position of a raster within a grid. This grid is a 2D plane
where raster positions are represented by a pair of coordinates. In GeoPySpark,
this is represented as a ``dict``.

The fields used to reprsent ``SpatialKey``:
 - **col** (int): The column of the grid, the numbers run east to west.
 - **row** (int): The row of the grid, the numbers run north to south.

Example::

   spatial_key = {'col': 0, 'row': 0}

SpaceTimeKey
------------

Represents the position of a raster within a grid. This grid is a 3D plane
where raster positions are represented by a pair of coordinates as well as a z
value that represents time. In GeoPySpark, this is represented as a ``dict``.

The fields used to reprsent ``SpaceTimeKey``:
 - **col** (int): The column of the grid, the numbers run east to west.
 - **row** (int): The row of the grid, the numbers run north to south.
 - **instance** (int): The time stamp of the raster.

Example::

   spatial_key = {'col': 0, 'row': 0, 'instant': 0.0}

Bounds
------

Represents the area covered by all of the values in a RDD on a grid. Uses
either ``SpatialKey`` s or ``SpaceTimeKey`` s depending on the type of data.
In GeoPySpark, this is represented as a ``dict``.

The fields used to represent ``Bounds``:
 - **minKey** (SpatialKey, SpaceTimeKey): The smallest SpatialKey or
       SpaceTimeKey.
 - **maxKey** (SpatialKey, SpaceTimeKey): The largest SpatialKey or
       SpaceTimeKey.

Example::

  min_key = {'col': 0, 'row': 0}
  max_key = {'col' 100', 'row': 100}

  bounds = {'minKey': min_key, 'max_key': max_key}

.. _metadata:

TileLayerMetadata
-----------------

Information on the values within a RDD. This is often needed when performing
certain actions. In GeoPySpark, this is represented as a ``dict``.

The fieldsd that are used to represent ``TileLayerMetadata``:
 - **cellType** (str): The type of all values in the rasters.
 - **layoutDefinition** (dict)
 - **extent** (Extent): The entire area of the source data.
 - **crs** (str): The CRS that the rasters are projected in.
 - **bounds** (Bounds): Represents the min and max boundary of the rasters.


How Data is Stored in RDDs
==========================

All data that is worked with in GeoPySpark is at somepoint stored within a RDD.
Therefore, it is important to understand how GeoPySpark stores, represents, and
uses these RDDs throughout the library.

GeoPySpark does not work with PySpark RDDs, but rather, uses python classes
that are wrappers of classes in scala that contain and work with a scala RDD.
The exact workings of this relationship between the python and scala classes
will not be discussed in this guide, instead the focus will be on what these
python classes represent and how they are used within GeoPySpark.

All RDDs in GeoPySpark contain tuples, which will be referred to in this guide
as ``(K, V)``. ``V`` will always be a raster, but ``K`` differs depending on
both the wrapper class and the nature of the data itself.

Where is the Actual RDD?
------------------------

The actual RDD that is being worked on exists in scala. Even if the RDD was
originally created in python, it will be serialized and sent over to scala
where it well decoded into scala RDD.

None of the operations performed on the RDD occur in python, and the only time
the RDD will be moved to python is if the user decideds to bring it over.

RasterRDD
----------

``RasterRDD`` is one of the two wrapper classes in GeoPySpark and deals with
untiled data. What does it mean for data to be untiled? It means that each
element within the RDD has not been modified in such a way that would make it
apart of a larger, overall layout. For example, a distributed collection of
rasters of a contiguous area could be derived from GeoTiffs of different sizes.
This, in turn, could mean that there's a lack of uniformity when viewing the
area as a whole. It is this, "raw" data that is stored within ``RasterRDD``.

It would help to have all of the data uniform when working with it, and that is
what ``RasterRDD`` accomplishes. The point of this class is to format the data
within the RDD to a specified layout.

As mentioned in the previous section, both wrapper classes hold data in tuples.
With the ``K`` of each tuple being different between the two. In the case of
``RasterRDD``, ``K`` is either ``ProjectedExtent``
or ``TemporalProjectedExtent``.

TiledRasterRDD
--------------

``TiledRasterRDD`` is the second of the two wrapper classes in GeoPySpark and
deals with tiled data. Which means the rasters inside of the RDD have been
fitted to a certain layout. The benefit of having data in this state is that
now it will be easy to work with. It is with this class that the user will be
able to perform map algebra, pyramid, and save the RDD among other operations.

As mentioned in the previous section, both wrapper classes hold data in tuples.
With the ``K`` of each tuple being different between the two. In the case of
``TiledRasterRDD``, ``K`` is either ``SpatialKey`` or ``SpaceTimeKey``.
