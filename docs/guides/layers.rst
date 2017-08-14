
.. code:: ipython3

    import datetime
    import numpy as np
    import pyproj
    import geopyspark as gps
    
    from pyspark import SparkContext
    from shapely.geometry import box

.. code:: ipython3

    !curl -o /tmp/cropped.tif https://s3.amazonaws.com/geopyspark-test/example-files/cropped.tif

.. code:: ipython3

    conf = gps.geopyspark_conf(master="local[*]", appName="layers")
    pysc = SparkContext(conf=conf)

Working With Layers
===================

How is Data Stored and Represented in GeoPySpark?
-------------------------------------------------

All data that is worked with in GeoPySpark is at some point stored
within an ``RDD``. Therefore, it is important to understand how
GeoPySpark stores, represents, and uses these ``RDD``\ s throughout the
library.

GeoPySpark does not work with PySpark ``RDD``\ s, but rather, uses
Python classes that are wrappers for Scala classes that contain and work
with a Scala ``RDD``. Specifically, these wrapper classes are
``RasterLayer`` and ``TiledRasterLayer``, which will be discussed in
more detail later.

Layers Are More Than RDDs
~~~~~~~~~~~~~~~~~~~~~~~~~

We refer to the Python wrapper classes as layers and not ``RDD``\ s for
two reasons: first, neither ``RasterLayer`` or ``TiledRasterLayer``
actually extends PySpark's ``RDD`` class; but more importantly, these
classes contain more information than just the ``RDD``. When we refer to
a “layer”, we mean both the ``RDD`` and its attributes.

The ``RDD``\ s contained by GeoPySpark layers contain tuples which have
type ``(K, V)``, where ``K`` represents the key, and ``V`` represents
the value. ``V`` will always be a ``Tile``, but ``K`` differs depending
on both the wrapper class and the nature of the data itself. More on
this below.

RasterLayer
~~~~~~~~~~~

The ``RasterLayer`` class deals with *untiled data*—that is, the
elements of the layer have not been normalized into a single unified
layout. Each raster element may have distinct resolutions or sizes; the
extents of the constituent rasters need not follow any orderly pattern.
Essentially, a ``RasterLayer`` stores “raw” data, and its main purpose
is to act as a way station on the path to acquiring *tiled data* that
adheres to a specified layout.

The ``RDD``\ s contained by ``RasterLayer`` objects have key type,
``K``, of either ``ProjectedExtent`` or ``TemporalProjectedExtent``,
when the layer type is ``SPATIAL`` or ``SPACETIME``, respectively.

TiledRasterLayer
~~~~~~~~~~~~~~~~

``TiledRasterLayer`` is the complement to ``RasterLayer`` and is meant
to store tiled data. Tiled data has been fitted to a certain layout,
meaning that it has been regularly sampled, and it has been cut up into
uniformly-sized, non-overlapping pieces that can be indexed sensibly.
The benefit of having data in this state is that now it will be easy to
work with. It is with this class that the user will be able to, for
example, perform map algebra, create pyramids, and save the layer. See
below for the definitions and specific examples of these operations.

In the case of ``TiledRasterLayer``, ``K`` is either ``SpatialKey`` or
``SpaceTimeKey``.

RasterLayer
-----------

Creating RasterLayers
~~~~~~~~~~~~~~~~~~~~~

There are just two ways to create a ``RasterLayer``: (1) through reading
GeoTiffs from the local file system, S3, or HDFS; or (2) from an
existing PySpark RDD.

From PySpark RDDs
^^^^^^^^^^^^^^^^^

The first option is to create a ``RasterLayer`` from a PySpark ``RDD``
via the ``from_numpy_rdd`` class method. This step can be a bit more
involved, as it requires the data within the PySpark RDD to be formatted
in a specific way (see `How is Data Stored and Represented in
GeoPySpark <#How-is-Data-Stored-and-Represented-in-GeoPySpark?>`__ for
more information).

The following example constructs an ``RDD`` from a tuple. The first
element is a ``ProjectedExtent`` because we have decided to make the
data spatial. If we were dealing with spatial-temproal data, then
``TemporalProjectedExtent`` would be the first element. A ``Tile`` will
always be the second element of the tuple.

.. code:: ipython3

    arr = np.ones((1, 16, 16), dtype='int')
    tile = gps.Tile.from_numpy_array(numpy_array=np.array(arr), no_data_value=-500)
    
    extent = gps.Extent(0.0, 1.0, 2.0, 3.0)
    projected_extent = gps.ProjectedExtent(extent=extent, epsg=3857)
    
    rdd = pysc.parallelize([(projected_extent, tile), (projected_extent, tile)])
    multiband_raster_layer = gps.RasterLayer.from_numpy_rdd(layer_type=gps.LayerType.SPATIAL, numpy_rdd=rdd)
    multiband_raster_layer

From GeoTiffs
^^^^^^^^^^^^^

The ``get`` function in the ``geopyspark.geotrellis.geotiff`` module
creates an instance of ``RasterLayer`` from GeoTiffs. These files can be
located on either your local file system, HDFS, or S3. In this example,
a GeoTiff with spatial data is read locally.

.. code:: ipython3

    raster_layer = gps.geotiff.get(layer_type=gps.LayerType.SPATIAL, uri="file:///tmp/cropped.tif")
    raster_layer

Using RasterLayer
~~~~~~~~~~~~~~~~~

This next section goes over the methods of ``RasterLayer``. It should be
noted that not all methods contained within this class will be covered.
More information on the methods that deal with the visualization of the
contents of the layer can be found in the [visualization guide].

Converting to a Python RDD
^^^^^^^^^^^^^^^^^^^^^^^^^^

By using ``to_numpy_rdd``, the base ``RasterLayer`` will be serialized
into a Python ``RDD``. This will convert all of the first values within
each tuple to either ``ProjectedExtent`` or ``TemporalProjectedExtent``,
and the second value to ``Tile``.

.. code:: ipython3

    python_rdd = raster_layer.to_numpy_rdd()
    python_rdd

.. code:: ipython3

    python_rdd.first()

SpaceTime Layer to Spatial Layer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're working with a spatial-temporal layer and would like to
convert it to a spatial layer, then you can use the ``to_spatial_layer``
method. This changes the keys of the ``RDD`` within the layer by
converting ``TemporalProjectedExtent`` to ``ProjectedExtent``.

.. code:: ipython3

    # Creating the space time layer
    
    instant = datetime.datetime.now()
    temporal_projected_extent = gps.TemporalProjectedExtent(extent=projected_extent.extent,
                                                            epsg=projected_extent.epsg,
                                                            instant=instant)
    
    space_time_rdd = pysc.parallelize([temporal_projected_extent, tile])
    space_time_layer = gps.RasterLayer.from_numpy_rdd(layer_type=gps.LayerType.SPACETIME, numpy_rdd=space_time_rdd)
    space_time_layer

.. code:: ipython3

    # Converting the SpaceTime layer to a Spatial layer
    
    space_time_layer.to_spatial_layer()

Collecting Metadata
^^^^^^^^^^^^^^^^^^^

The ``Metadata`` of a layer contains information of the values within
it. This data pertains to the layout, projection, and extent of the data
found within the layer.

``collect_metadata`` will return the ``Metadata`` of the layer that fits
the ``layout`` given.

.. code:: ipython3

    # Collecting Metadata with the default LocalLayout()
    metadata = raster_layer.collect_metadata()
    metadata

.. code:: ipython3

    # Collecting Metadata with the default GlobalLayout()
    raster_layer.collect_metadata(layout=gps.GlobalLayout())

.. code:: ipython3

    # Collecting Metadata with a LayoutDefinition
    extent = gps.Extent(0.0, 0.0, 33.0, 33.0)
    tile_layout = gps.TileLayout(2, 2, 256, 256)
    layout_definition = gps.LayoutDefinition(extent, tile_layout)
    
    raster_layer.collect_metadata(layout=layout_definition)

Reproject
^^^^^^^^^

``reproject`` will change the projection the rasters within the layer to
the given ``target_crs``. This method does not sample past the tiles'
boundaries.

.. code:: ipython3

    # The CRS of the layer before reprojecting
    metadata.crs

.. code:: ipython3

    # The CRS of the layer after reprojecting
    raster_layer.reproject(target_crs=3857).collect_metadata().crs

Tiling Data to a Layout
^^^^^^^^^^^^^^^^^^^^^^^

``tile_to_layout`` will tile and format the rasters within a
``RasterLayer`` to a given layout. The result of this tiling is a new
instance of ``TiledRasterLayer``. This output contains the same data as
its source ``RasterLayer``, however, the information contained within it
will now be orginized according to the given layout.

During this step it is also possible to reproject the ``RasterLayer``.
This can be done by specifying the ``target_crs`` to reproject to.
Reprojecting using this method produces a different result than what is
returned by the ``reproject`` method. Whereas the latter does not sample
past the boundaries of rasters within the layer, the former does. This
is important as anything with a ``GlobalLayout`` needs to sample past
the boundaries of the rasters.

From Metadata
'''''''''''''

Create a ``TiledRasterLayer`` that contains the layout from the given
``Metadata``.

**Note**: If the specified ``target_crs`` is different from what's in
the metadata, then an error will be thrown.

.. code:: ipython3

    raster_layer.tile_to_layout(layout=metadata)

From LayoutDefinition
'''''''''''''''''''''

.. code:: ipython3

    raster_layer.tile_to_layout(layout=layout_definition)

From LocalLayout
''''''''''''''''

.. code:: ipython3

    raster_layer.tile_to_layout(gps.LocalLayout())

From GlobalLayout
'''''''''''''''''

.. code:: ipython3

    tiled_raster_layer = raster_layer.tile_to_layout(gps.GlobalLayout())
    tiled_raster_layer

From A TiledRasterLayer
'''''''''''''''''''''''

One can tile a ``RasterLayer`` to the same layout as a
``TiledRasterLayout``.

**Note**: If the specifying ``target_crs`` is different from the other
layer's, then an error will be thrown.

.. code:: ipython3

    raster_layer.tile_to_layout(layout=tiled_raster_layer)

TiledRasterLayer
----------------

Creating TiledRasterLayers
~~~~~~~~~~~~~~~~~~~~~~~~~~

For this guide, we will just go over one initialization method for
``TiledRasterLayer``, ``from_numpy_rdd``. However, there are other ways
to create this class. These additional creation strategies can be found
in the [map algebra guide].

From PySpark RDD
^^^^^^^^^^^^^^^^

Like ``RasterLayer``\ s, ``TiledRasterLayer``\ s can be created from
``RDD``\ s using ``from_numpy_rdd``. What is different, however, is that
``Metadata`` must also be passed in during initialization. This makes
creating ``TiledRasterLayer``\ s this way a little bit more arduous.

The following example constructs an ``RDD`` from a tuple. The first
element is a ``SpatialKey`` because we have decided to make the data
spatial. If we were dealing with spatial-temproal data, then
``SpaceTimeKey`` would be the first element. ``Tile`` will always be the
second element of the tuple.

.. code:: ipython3

    data = np.zeros((1, 512, 512), dtype='float32')
    tile = gps.Tile.from_numpy_array(numpy_array=data, no_data_value=-1.0)
    instant = datetime.datetime.now()
    
    layer = [(gps.SpaceTimeKey(row=0, col=0, instant=instant), tile),
             (gps.SpaceTimeKey(row=1, col=0, instant=instant), tile),
             (gps.SpaceTimeKey(row=0, col=1, instant=instant), tile),
             (gps.SpaceTimeKey(row=1, col=1, instant=instant), tile)]
    
    rdd = pysc.parallelize(layer)
    
    extent = gps.Extent(0.0, 0.0, 33.0, 33.0)
    layout = gps.TileLayout(2, 2, 512, 512)
    bounds = gps.Bounds(gps.SpaceTimeKey(col=0, row=0, instant=instant), gps.SpaceTimeKey(col=1, row=1, instant=instant))
    layout_definition = gps.LayoutDefinition(extent, layout)
    
    metadata = gps.Metadata(
        bounds=bounds,
        crs='+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ',
        cell_type='float32ud-1.0',
        extent=extent,
        layout_definition=layout_definition)
    
    space_time_tiled_layer = gps.TiledRasterLayer.from_numpy_rdd(layer_type=gps.LayerType.SPACETIME,
                                                                 numpy_rdd=rdd, metadata=metadata)
    space_time_tiled_layer

Using TiledRasterLayers
~~~~~~~~~~~~~~~~~~~~~~~

This section will go over the methods found within ``TiledRasterLayer``.
Like with ``RasterLayer``, not all methods within this class will be
covered in this guide. More information on the methods that deal with
the visualization of the contents of the layer can be found in the
[visualization guide]; and those that deal with map algebra can be found
in the [map algebra guide].

Converting to a Python RDD
^^^^^^^^^^^^^^^^^^^^^^^^^^

By using ``to_numpy_rdd``, the base ``TiledRasterLayer`` will be
serialized into a Python ``RDD``. This will convert all of the first
values within each tuple to either ``SpatialKey`` or ``SpaceTimeKey``,
and the second value to ``Tile``.

.. code:: ipython3

    python_rdd = tiled_raster_layer.to_numpy_rdd()

.. code:: ipython3

    python_rdd.first()

SpaceTime Layer to Spatial Layer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you're working with a spatiotemporal layer and would like to convert
it to a spatial layer, then you can use the ``to_spatial_layer`` method.
This changes the keys of the ``RDD`` within the layer by converting
``SpaceTimeKey`` to ``SpatialKey``.

.. code:: ipython3

    # Creating the space time layer
    
    instant = datetime.datetime.now()
    space_time_key = gps.SpaceTimeKey(col=0, row=0, instant=instant)
    
    metadata = gps.Metadata(
        bounds=gps.Bounds(space_time_key, space_time_key),
        cell_type='int16',
        crs = '+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ',
        extent=extent,
        layout_definition=layout_definition)
    
    space_time_rdd = pysc.parallelize([space_time_key, tile])
    space_time_layer = gps.TiledRasterLayer.from_numpy_rdd(layer_type=gps.LayerType.SPACETIME,
                                                           numpy_rdd=space_time_rdd,
                                                           metadata=metadata)
    space_time_layer

.. code:: ipython3

    # Converting the SpaceTime layer to a Spatial layer
    
    space_time_layer.to_spatial_layer()

Repartitioning
^^^^^^^^^^^^^^

While not an ``RDD``, ``TiledRasterLayer`` does contain an underlying
``RDD``, and thus, it can be repartitioned using the ``repartition``
method.

.. code:: ipython3

    # Repartition the internal RDD to have 120 partitions
    tiled_raster_layer.repartition(num_partitions=120)

Lookup
^^^^^^

If there is a particular tile within the layer that is of interest, it
is possible to retrieve it as a ``Tile`` using the ``lookup`` method.

.. code:: ipython3

    min_key = tiled_raster_layer.layer_metadata.bounds.minKey
    
    # Retrieve the Tile that is located at the smallest column and row of the layer
    tiled_raster_layer.lookup(col=min_key.col, row=min_key.row)

Masking
^^^^^^^

By using ``mask`` method, the ``TiledRasterRDD`` can be masekd using one
or more Shapely geometries.

.. code:: ipython3

    layer_extent = tiled_raster_layer.layer_metadata.extent
    
    # Polygon to mask a region of the layer
    mask = box(layer_extent.xmin,
               layer_extent.ymin,
               layer_extent.xmin + 20,
               layer_extent.ymin + 20)
    
    tiled_raster_layer.mask(geometries=mask)

.. code:: ipython3

    mask_2 = box(layer_extent.xmin + 50,
                 layer_extent.ymin + 50,
                 layer_extent.xmax - 20,
                 layer_extent.ymax - 20)
    
    # Multiple Polygons can be given to mask the layer
    tiled_raster_layer.mask(geometries=[mask, mask_2])

Normalize
^^^^^^^^^

``normalize`` will linearly transform the data within the layer such
that all values fall within a given range.

.. code:: ipython3

    # Normalizes the layer so that the new min value is 0 and the new max value is 60000
    tiled_raster_layer.normalize(new_min=0, new_max=60000)

Pyramiding
^^^^^^^^^^

When using a layer for a TMS server, it is important that the layer is
pyramided. That is, we create a level-of-detail hierarchy that covers
the same geographical extent, while each level of the pyramid uses one
quarter as many pixels as the next level. This allows us to zoom in and
out when the layer is being displayed without using extraneous detail.
The ``pyramid`` method will produce an instance of ``Pyramid`` that will
contain within it multiple ``TiledRasterLayer``\ s. Each layer
corresponds to a zoom level, and the number of levels depends on the
``zoom_level`` of the source layer. With the max zoom of the ``Pyramid``
being the source layer's ``zoom_level``, and the lowest zoom being 0.

For more information on the ``Pyramiding`` class, see the [visualization
guide].

.. code:: ipython3

    # This creates a Pyramid with zoom levels that go from 0 to 11 for a total of 12.
    tiled_raster_layer.pyramid()

Reproject
^^^^^^^^^

This is similar to the ``reproject`` method for ``RasterLayer`` where
the reprojection will not sample past the tiles' boundaries. This means
the layout of the tiles will be changed so that they will take on a
``LocalLayout`` rather than a ``GlobalLayout`` (read more about these
layouts `here <core-concepts.ipynb#Tiling-Strategies>`__). Because of
this, whatever ``zoom_level`` the ``TiledRasterLayer`` has will be
changed to 0 since the area being represented changes to just the tiles.

.. code:: ipython3

    # The zoom_level and crs of the TiledRasterLayer before reprojecting
    tiled_raster_layer.zoom_level, tiled_raster_layer.layer_metadata.crs

.. code:: ipython3

    reprojected_tiled_raster_layer = tiled_raster_layer.reproject(target_crs=3857)
    
    # The zoom_level and crs of the TiledRasterLayer after reprojecting
    reprojected_tiled_raster_layer.zoom_level, reprojected_tiled_raster_layer.layer_metadata.crs

Stitching
^^^^^^^^^

Using ``stitch`` will produce a single ``Tile`` by stitching together
all of the tiles within the ``TiledRasterLayer``. This can only be done
with spatial layers, and is not recommended if the data contained within
the layer is large, as it can cause a crash due to the size of the
resulting ``Tile``.

.. code:: ipython3

    # Creates a Tile with an underlying numpy array with a size of (1, 6144, 1536).
    tiled_raster_layer.stitch().cells.shape

Saving a Stitched Layer
^^^^^^^^^^^^^^^^^^^^^^^

The ``save_stitched`` method both stitches and saves a layer as a
GeoTiff.

.. code:: ipython3

    # Saves the stitched layer to /tmp/stitched.tif
    tiled_raster_layer.save_stitched(path='/tmp/stitched.tif')

It is also possible to specify the regions of layer to be saved when it
is stitched.

.. code:: ipython3

    layer_extent = tiled_raster_layer.layer_metadata.layout_definition.extent
    
    # Only a portion of the stitched layer needs to be saved, so we will create a sub Extent to crop to.
    sub_exent = gps.Extent(xmin=layer_extent.xmin + 10,
                           ymin=layer_extent.ymin + 10,
                           xmax=layer_extent.xmax - 10,
                           ymax=layer_extent.ymax - 10)
    
    tiled_raster_layer.save_stitched(path='/tmp/cropped-stitched.tif', crop_bounds=sub_exent)

.. code:: ipython3

    # In addition to the sub Extent, one can also choose how many cols and rows will be in the saved in the GeoTiff.
    tiled_raster_layer.save_stitched(path='/tmp/cropped-stitched-2.tif',
                                     crop_bounds=sub_exent,
                                     crop_dimensions=(1000, 1000))

Tiling Data to a Layout
^^^^^^^^^^^^^^^^^^^^^^^

This is similar to ``RasterLayer``'s ``tile_to_layout`` method, except
for one important detail. If performing a ``tile_to_layout`` on a
``TiledRasterLayer`` that contains a ``zoom_level``, that ``zoom_level``
could be lost or changed depending on the ``layout`` and/or
``target_crs`` chosen. Thus, it is important to keep that in mind in
retiling a ``TiledRasterLayer``.

.. code:: ipython3

    # Original zoom_level of the source TiledRasterLayer
    tiled_raster_layer.zoom_level

.. code:: ipython3

    # zoom_level will be lost in the resulting TiledRasterlayer
    tiled_raster_layer.tile_to_layout(layout=gps.LocalLayout())

.. code:: ipython3

    # zoom_level will be changed in the resulting TiledRasterLayer
    tiled_raster_layer.tile_to_layout(layout=gps.GlobalLayout(), target_crs=3857)

.. code:: ipython3

    # zoom_level will reamin the same in the resulting TiledRasterLayer
    tiled_raster_layer.tile_to_layout(layout=gps.GlobalLayout(zoom=11))

General Methods
---------------

There exist methods that are found in both ``RasterLayer`` and
``TiledRasterLayer``. These methods tend to perform more general
analysis/tasks, thus making them suitable for both classes. This next
section will go over these methods.

**Note**: In the following examples, both ``RasterLayer``\ s and
``TiledRasterLayer``\ s will be used. However, they can easily be
subsituted with the other class.

Selecting a SubSection of Bands
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To select certain bands to work with, the ``bands`` method will take
either a single or collection of band indices and will return the subset
as a new ``RasterLayer`` or ``TiledRasterLayer``.

**Note**: There could high performance costs if operations are performed
between two sub-bands of a large dataset. Thus, if you're working with a
large amount of data, then it is recommended to do band selection before
reading them in.

.. code:: ipython3

    # Selecting the second band from the layer
    multiband_raster_layer.bands(1)

.. code:: ipython3

    # Selecting the first and second bands from the layer
    multiband_raster_layer.bands([0, 1])

Converting the Data Type of the Rasters' Cells
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``convert_data_type`` method will convert the types of the cells
within the rasters of the layer to a new data type. The ``noData`` value
can also be set during this conversion, and if it's not set, then there
will be no ``noData`` value for the resulting rasters.

.. code:: ipython3

    # The data type of the cells before converting
    metadata.cell_type

.. code:: ipython3

    # Changing the cell type to int8 with a noData value of -100.
    raster_layer.convert_data_type(new_type=gps.CellType.INT8, no_data_value=-100).collect_metadata().cell_type

.. code:: ipython3

    # Changing the cell type to int32 with no noData value.
    raster_layer.convert_data_type(new_type=gps.CellType.INT32).collect_metadata().cell_type

Reclassify Cell Values
~~~~~~~~~~~~~~~~~~~~~~

``reclassify`` changes the cell values based on the ``value_map`` and
``classification_strategy`` given. In addition to these two parameters,
the ``data_type`` of the cells also needs to be given. This is either
``int`` or ``float``.

.. code:: ipython3

    # Values of the first tile before being reclassified
    multiband_raster_layer.to_numpy_rdd().first()[1]

.. code:: ipython3

    # Change all values greater than or equal to 1 to 10
    reclassified = multiband_raster_layer.reclassify(value_map={1: 10},
                                                     data_type=int,
                                                     classification_strategy=gps.ClassificationStrategy.GREATER_THAN_OR_EQUAL_TO)
    reclassified.to_numpy_rdd().first()[1]

Mapping Over the Cells
~~~~~~~~~~~~~~~~~~~~~~

It is possible to work with the cells within a layer directly via the
``map_cells`` method. This method takes a function that expects a numpy
array and a noData value as parameters, and returns a new numpy array.
Thus, the function given would have the following type signature:

.. code:: python

    def input_function(numpy_array: np.ndarray, no_data_value=None) -> np.ndarray

The given function is then applied to each ``Tile`` in the layer.

**Note**: In order for this method to operate, the internal ``RDD``
first needs to be deserialized from Scala to Python and then serialized
from Python back to Scala. Because of this, it is recommended to chain
together all functions to avoid unnecessary serialization overhead.

.. code:: ipython3

    def add_one(cells, _):
        return cells + 1
    
    # Mapping with a single funciton
    raster_layer.map_cells(add_one)

.. code:: ipython3

    def divide_two(cells, _):
        return (add_one(cells) / 2)
    
    # Chaning together two functions to be mapped
    raster_layer.map_cells(divide_two)

Mapping Over Tiles
~~~~~~~~~~~~~~~~~~

Like ``map_cells``, ``map_tiles`` maps a given function over all of the
``Tile``\ s within the layer. It takes a function that expects a
``Tile`` and returns a ``Tile``. Therefore, the input function's type
signature would be this:

.. code:: python

    def input_function(tile: Tile) -> Tile

**Note**: In order for this method to operate, the internal ``RDD``
first needs to be deserialized from Scala to Python and then serialized
from Python back to Scala. Because of this, it is recommended to chain
together all functions to avoid unnecessary serialization overhead.

.. code:: ipython3

    def minus_two(tile):
        return gps.Tile.from_numpy_array(tile.cells - 2, no_data_value=tile.no_data_value)
    
    raster_layer.map_tiles(minus_two)

Calculating the Histogram for the Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is possible to calculate the histogram of a layer either by using the
``get_histogram`` or the ``get_class_histogram`` method. Both of these
methods produce a ``Histogram``, however, the way the data is
represented within the resulting histogram differs depending on the
method used. ``get_histogram`` will produce a histogram whose values are
``float``\ s. Whereas ``get_class_histogram`` returns a histogram whose
values are ``int``\ s.

For more informaiton on the ``Histogram`` class, please see the
``Histogram`` [guide].

.. code:: ipython3

    # Returns a Histogram whose underlying values are floats
    tiled_raster_layer.get_histogram()

.. code:: ipython3

    # Returns a Histogram whose underlying values are ints
    tiled_raster_layer.get_class_histogram()

Finding the Quantile Breaks for the Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you wish to find the quantile breaks for a layer without a
``Histogram``, then you can use the ``get_quantile_breaks`` method.

.. code:: ipython3

    tiled_raster_layer.get_quantile_breaks(num_breaks=3)

Quantile Breaks for Exact Ints
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is another version of ``get_quantile_breaks`` called
``get_quantile_breaks_exact_int`` that will count exact integer values.
However, if there are too many values within the layer, then memory
errors could occur.

.. code:: ipython3

    tiled_raster_layer.get_quantile_breaks_exact_int(num_breaks=3)

Finding the Min and Max Values of a Layer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``get_min_max`` method will find the min and max value for the
layer. The result will always be ``(float, float)`` regardless of the
data type of the cells.

.. code:: ipython3

    tiled_raster_layer.get_min_max()

RDD Methods
-----------

As mentioned in the section on ``TiledRasterLayer``'s `repartition
method <#repartitioning>`__, ``TiledRasterLayer`` has methods to work
with its internal ``RDD``. This holds true for ``RasterLayer`` as well.

The following is a list of ``RDD`` with examples that are supported by
both classes.

Cache
~~~~~

.. code:: ipython3

    raster_layer.cache()

Persist
~~~~~~~

.. code:: ipython3

    # If no level is given, then MEMORY_ONLY will be used
    tiled_raster_layer.persist()

Unpersist
~~~~~~~~~

.. code:: ipython3

    tiled_raster_layer.unpersist()

getNumberOfPartitions
~~~~~~~~~~~~~~~~~~~~~

.. code:: ipython3

    raster_layer.getNumPartitions()

Count
~~~~~

.. code:: ipython3

    raster_layer.count()
