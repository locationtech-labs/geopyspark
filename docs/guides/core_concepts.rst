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
Python's more traditional naming convention for some values. This is because
Scala uses this style of naming, and when it receives data from Python it
expects the value names to be in camel case.

.. _raster:

Raster
------

GeoPySpark differs in how it represents rasters from other geo-spatial Python
libraries like rasterio. In GeoPySpark, they are represented as a ``dict``.

The fields used to represent rasters:
 - **no_data_value**: The value that represents no data in raster. This can be
   represented by a variety of types depending on the value type of the raster.
 - **data** (nd.array): The raster data itself. It is contained within a NumPy
   array.

**Note**: All rasters in GeoPySpark are represented as having multiple bands,
even if the original raster just contained one.

.. _projected_extent:

ProjectedExtent
---------------

Describes both the area on Earth a raster represents in addition to its CRS.
In GeoPySpark, this is represented as a ``dict``.

The fields used to represent ``ProjectedExtent``:
 - **extent** (:obj:`~geopyspark.geotrellis.data_structures.Extent`): The area the raster
   represents.
 - **epsg** (int, optional): The EPSG code of the CRS.
 - **proj4** (str, optional): The Proj.4 string representation of the CRS.

Example:

.. code:: python

   extent = Extent(0.0, 1.0, 2.0, 3.0)

   # using epsg
   epsg_code = 3857
   projected_extent = {'extent': extent, 'epsg': epsg}

   # using proj4
   proj4 = "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs "
   projected_extent = {'extent': extent, 'proj4': proj4}


**Note**: Either ``epsg`` or ``proj4`` must be defined.

.. _temporal_extent:

TemporalProjectedExtent
-----------------------

Describes the area on Earth the raster represents, its CRS, and the time the
data was collected. In GeoPySpark, this is represented as a ``dict``.

The fields used to represent ``TemporalProjectedExtent``.
 - **extent** (:obj:`~geopyspark.geotrellis.data_structures.Extent`): The area the raster
   represents.
 - **epsg** (int, optional): The EPSG code of the CRS.
 - **proj4** (str, optional): The Proj.4 string representation of the CRS.
 - **instance** (int): The time stamp of the raster.

Example:

.. code:: python

   extent = Extent(0.0, 1.0, 2.0, 3.0)

   epsg_code = 3857
   instance = 1.0
   projected_extent = {'extent': extent, 'epsg': epsg, 'instance': instance}

**Note**: Either ``epsg`` or ``proj4`` must be defined.

.. _spatial-key:

SpatialKey
----------

Represents the position of a raster within a grid. This grid is a 2D plane
where raster positions are represented by a pair of coordinates. In GeoPySpark,
this is represented as a ``dict``.

The fields used to represent ``SpatialKey``:
 - **col** (int): The column of the grid, the numbers run east to west.
 - **row** (int): The row of the grid, the numbers run north to south.

Example:

.. code:: python

   spatial_key = {'col': 0, 'row': 0}

.. _space-time-key:

SpaceTimeKey
------------

Represents the position of a raster within a grid. This grid is a 3D plane
where raster positions are represented by a pair of coordinates as well as a z
value that represents time. In GeoPySpark, this is represented as a ``dict``.

The fields used to reprsent ``SpaceTimeKey``:
 - **col** (int): The column of the grid, the numbers run east to west.
 - **row** (int): The row of the grid, the numbers run north to south.
 - **instance** (int): The time stamp of the raster.

Example:

.. code:: python

   spatial_key = {'col': 0, 'row': 0, 'instant': 0.0}


.. _data_rep:

How Data is Stored in RDDs
==========================

All data that is worked with in GeoPySpark is at some point stored within a RDD.
Therefore, it is important to understand how GeoPySpark stores, represents, and
uses these RDDs throughout the library.

GeoPySpark does not work with PySpark RDDs, but rather, uses Python classes
that are wrappers of classes in Scala that contain and work with a Scala RDD.
The exact workings of this relationship between the Python and Scala classes
will not be discussed in this guide, instead the focus will be on what these
Python classes represent and how they are used within GeoPySpark.

All RDDs in GeoPySpark contain tuples, which will be referred to in this guide
as ``(K, V)``. ``V`` will always be a raster, but ``K`` differs depending on
both the wrapper class and the nature of the data itself.

Where is the Actual RDD?
------------------------

The actual RDD that is being worked on exists in Scala. Even if the RDD was
originally created in Python, it will be serialized and sent over to Scala
where it will be decoded into a Scala RDD.

None of the operations performed on the RDD occur in Python, and the only time
the RDD will be moved to Python is if the user decides to bring it over.

.. _raster_rdd:

RasterRDD
----------

``RasterRDD`` is one of the two wrapper classes in GeoPySpark and deals with
untiled data. What does it mean for data to be untiled? It means that each
element within the RDD has not been modified in such a way that would make it
a part of a larger, overall layout. For example, a distributed collection of
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

.. _tiled-raster-rdd:

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
