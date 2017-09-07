
.. code:: python3

   import datetime
   import geopyspark as gps
   import numpy as np
    
   from pyspark import SparkContext
   from shapely.geometry import MultiPolygon, box

.. code:: python3

   !curl -o /tmp/cropped.tif https://s3.amazonaws.com/geopyspark-test/example-files/cropped.tif

.. code:: python3

   conf = gps.geopyspark_conf(master="local[*]", appName="layers")
   pysc = SparkContext(conf=conf)

.. code:: python3

   # Setting up the Spatial Data to be used in this example
    
   spatial_raster_layer = gps.geotiff.get(layer_type=gps.LayerType.SPATIAL, uri="/tmp/cropped.tif")
   spatial_tiled_layer = spatial_raster_layer.tile_to_layout(layout=gps.GlobalLayout(), target_crs=3857)

.. code:: python3

   # Setting up the Spatial-Temporal Data to be used in this example
    
   def make_raster(x, y, v, cols=4, rows=4, crs=4326):
       cells = np.zeros((1, rows, cols), dtype='float32')
       cells.fill(v)
       # extent of a single cell is 1
       extent = gps.TemporalProjectedExtent(extent = gps.Extent(x, y, x + cols, y + rows),
                                            epsg=crs,
                                            instant=datetime.datetime.now())
        
       return (extent, gps.Tile.from_numpy_array(cells))
                
   layer = [
       make_raster(0, 0, v=1),
       make_raster(3, 2, v=2),
       make_raster(6, 0, v=3)
   ]
      
   rdd = pysc.parallelize(layer)
   space_time_raster_layer = gps.RasterLayer.from_numpy_rdd(gps.LayerType.SPACETIME, rdd)
   space_time_tiled_layer = space_time_raster_layer.tile_to_layout(layout=gps.GlobalLayout(tile_size=5))
   space_time_pyramid = space_time_tiled_layer.pyramid()

Catalog
=======

The ``catalog`` module allows for users to retrieve information, query,
and write to/from GeoTrellis layers.

What is a Catalog?
------------------

A catalog is a directory where saved layers and their attributes are
organized and stored in a certain manner. Within a catalog, there can
exist multiple layers from different data sets. Each of these layers, in
turn, are their own directories which contain two folders: one where the
data is stored and the other for the metadata. The data for each layer
is broken up into zoom levels and each level has its own folder within
the data folder of the layer. As for the metadata, it is also broken up
by zoom level and is stored as ``json`` files within the metadata
folder.

Here's an example directory structure of a catalog:

::

    layer_catalog/
      layer_a/
        metadata_for_layer_a/
          metadata_layer_a_zoom_0.json
          ....
        data_for_layer_a/
          0/
            data
            ...
          1/
            data
            ...
          ...
      layer_b/
      ...

Accessing Data
--------------

GeoPySpark supports a number of different backends to save and read
information from. These are the currently supported backends:

-  LocalFileSystem
-  HDFS
-  S3
-  Cassandra
-  HBase
-  Accumulo

Each of these needs to be accessed via the ``URI`` for the given system.
Here are example ``URI``\ s for each:

-  **Local Filesystem**: file://my\_folder/my\_catalog/
-  **HDFS**: hdfs://my\_folder/my\_catalog/
-  **S3**: s3://my\_bucket/my\_catalog/
-  **Cassandra**:
   cassandra://[user:password@]zookeeper[:port][/keyspace][?attributes=table1[&layers=table2]]
-  **HBase**:
   hbase://zookeeper[:port][?master=host][?attributes=table1[&layers=table2]]
-  **Accumulo**:
   accumulo://[user[:password]@]zookeeper/instance-name[?attributes=table1[&layers=table2]]

It is important to note that neither HBase nor Accumulo have native
support for ``URI``\ s. Thus, GeoPySpark uses its own pattern for these
two systems.

A Note on Formatting Tiles
~~~~~~~~~~~~~~~~~~~~~~~~~~

A small, but important, note needs to be made about how tiles that are
saved and/or read in are formatted in GeoPySpark. All tiles will be
treated as a ``MultibandTile``. Regardless if they were one to begin
with. This was a design choice that was made to simplify both the
backend and the API of GeoPySpark.

Saving Data to a Backend
------------------------

The ``write`` function will save a given ``TiledRasterLayer`` to a
specified backend. If the catalog does not exist when calling this
function, then it will be created along with the saved layer.

**Note**: It is not possible to save a layer to a catalog if the layer
name and zoom already exist. If you wish to overwrite an existing, saved
layer then it must be deleted before writing the new one.

**Note**: Saving a ``TiledRasterLayer`` that does not have a
``zoom_level`` will save the layer to a zoom of 0. Thus, when it is read
back out from the catalog, the resulting ``TiledRasterLayer`` will have
a ``zoom_level`` of 0.

Saving a Spatial Layer
~~~~~~~~~~~~~~~~~~~~~~

Saving a spatial layer is a straight forward task. All that needs to be
supplied is a ``URI``, the name of the layer, and the layer to be saved.

.. code:: python3

    # The zoom level which will be saved
    spatial_tiled_layer.zoom_level

.. code:: python3

    # This will create a catalog called, "spatial-catalog" in the /tmp directory.
    # Within it, a layer named, "spatial-layer" will be saved.
    gps.write(uri='file:///tmp/spatial-catalog', layer_name='spatial-layer', tiled_raster_layer=spatial_tiled_layer)

Saving a Spatial Temporal Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When saving a spatial-temporal layer, one needs to consider how the
records within the catalog will be spaced; which in turn, determines the
resolution of index. The ``TimeUnit`` enum class contains all available
units of time that can be used to space apart data in the catalog.

.. code:: python3

    # The zoom level which will be saved
    space_time_tiled_layer.zoom_level

.. code:: python3

    # This will create a catalog called, "spacetime-catalog" in the /tmp directory.
    # Within it, a layer named, "spacetime-layer" will be saved and each indice will be spaced apart by SECONDS
    gps.write(uri='file:///tmp/spacetime-catalog',
              layer_name='spacetime-layer',
              tiled_raster_layer=space_time_tiled_layer,
              time_unit=gps.TimeUnit.SECONDS)

Saving a Pyramid
~~~~~~~~~~~~~~~~

For those that are unfamiliar with the ``Pyramid`` class, please see the
[Pyramid section] of the visualization guide. Otherwise, please continue
on.

As of right now, there is no way to directly save a ``Pyramid``.
However, because a ``Pyramid`` is just a collection of
``TiledRasterLayer``\ s of different zooms, it is possible to iterate
through the layers of the ``Pyramid`` and save one individually.

.. code:: python3

    for zoom, layer in space_time_pyramid.levels.items():
        # Because we've already written a layer of the same name to the same catalog with a zoom level of 7,
        # we will skip writing the level 7 layer.
        if zoom != 7:
            gps.write(uri='file:///tmp/spacetime-catalog',
                      layer_name='spacetime-layer',
                      tiled_raster_layer=layer,
                      time_unit=gps.TimeUnit.SECONDS)

Reading Metadata From a Saved Layer
-----------------------------------

It is possible to retrieve the ``Metadata`` for a layer without reading
in the whole layer. This is done using the ``read_layer_metadata``
function. There is no difference between spatial and spatial-temporal
layers when using this function.

.. code:: python3

    # Metadata from the TiledRasterLayer
    spatial_tiled_layer.layer_metadata

.. code:: python3

    # Reads the Metadata from the spatial-layer of the spatial-catalog for zoom level 11
    gps.read_layer_metadata(uri="file:///tmp/spatial-catalog",
                            layer_name="spatial-layer",
                            layer_zoom=11)

Reading a Tile From a Saved Layer
---------------------------------

One can read a single tile that has been saved to a layer using the
``read_value`` function. This will either return a ``Tile`` or ``None``
depending on whether or not the specified tile exists.

Reading a Tile From a Saved, Spatial Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python3

    # The Tile being read will be the smallest key of the layer
    min_key = spatial_tiled_layer.layer_metadata.bounds.minKey
    
    gps.read_value(uri="file:///tmp/spatial-catalog",
                   layer_name="spatial-layer",
                   layer_zoom=11,
                   col=min_key.col,
                   row=min_key.row)

Reading a Tile From a Saved, Spatial-Temporal Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python3

    # The Tile being read will be the largest key of the layer
    max_key = space_time_tiled_layer.layer_metadata.bounds.maxKey
    
    gps.read_value(uri="file:///tmp/spacetime-catalog",
                   layer_name="spacetime-layer",
                   layer_zoom=7,
                   col=max_key.col,
                   row=max_key.row,
                   zdt=max_key.instant)

Reading a Layer
---------------

There are two ways one can read a layer in GeoPySpark: reading the
entire layer or just portions of it. The former will be the goal
discussed in this section. While all of the layer will be read, the
function for doing so is called, ``query``. There is no difference
between spatial and spatial-temporal layers when using this function.

**Note**: What distinguishes between a full and partial read is the
parameters given to ``query``. If no filters were given, then the whole
layer is read.

.. code:: python3

    # Returns the entire layer that was at zoom level 11.
    gps.query(uri="file:///tmp/spatial-catalog",
              layer_name="spatial-layer",
              layer_zoom=11)

Querying a Layer
----------------

When only a certain section of the layer is of interest, one can
retrieve these areas of the layer through the ``query`` method.
Depending on the type of data being queried, there are a couple of ways
to filter what will be returned.

Querying a Spatial Layer
~~~~~~~~~~~~~~~~~~~~~~~~

One can query an area of a spatial layer that covers the region of
interest by providing a geometry that represents this region. This area
can be represented as: ``shapely.geometry`` (specifically ``Polygon``\ s
and ``MultiPolygon``\ s), the ``wkb`` representation of the geometry, or
an ``Extent``.

**Note**: It is important that the given geometry is in the same
projection as the queried layer. Otherwise, either the wrong area or
nothing will be returned.

When the Queried Geometry is in the Same Projection as the Layer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By default, the ``query`` function assumes that the geometry and layer
given are in the same projection.

.. code:: python3

    layer_extent = spatial_tiled_layer.layer_metadata.extent
    
    # Creates a Polygon from the cropped Extent of the Layer
    poly = box(layer_extent.xmin+100, layer_extent.ymin+100, layer_extent.xmax-100, layer_extent.ymax-100)

.. code:: python3

    # Returns the region of the layer that was intersected by the Polygon at zoom level 11.
    gps.query(uri="file:///tmp/spatial-catalog",
              layer_name="spatial-layer",
              layer_zoom=11,
              query_geom=poly)

When the Queried Geometry is in a Different Projection than the Layer
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As stated above, it is important that both the geometry and layer are in
the same projection. If the two are in different CRSs, then this can be
resolved by setting the ``proj_query`` parameter to whatever projection
the geometry is in.

.. code:: python3

    # The queried Extent is in a different projection than the base layer
    metadata = spatial_tiled_layer.tile_to_layout(layout=gps.GlobalLayout(), target_crs=4326).layer_metadata
    metadata.layout_definition.extent, spatial_tiled_layer.layer_metadata.layout_definition.extent

.. code:: python3

    # Queries the area of the Extent and returns any intersections
    querried_spatial_layer = gps.query(uri="file:///tmp/spatial-catalog",
                                       layer_name="spatial-layer",
                                       layer_zoom=11,
                                       query_geom=metadata.layout_definition.extent.to_polygon,
                                       query_proj="EPSG:3857")

.. code:: python3

    # Because we queried the whole Extent of the layer, we should have gotten back the whole thing.
    querried_extent = querried_spatial_layer.layer_metadata.layout_definition.extent
    base_extent = spatial_tiled_layer.layer_metadata.layout_definition.extent
    
    querried_extent == base_extent

Querying a Spatial-Temporal Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to being able to query a geometry, spatial-temporal data can
also be filtered by time as well.

Querying by Time
^^^^^^^^^^^^^^^^

.. code:: python3

    min_key = space_time_tiled_layer.layer_metadata.bounds.minKey
    
    # Returns a TiledRasterLayer whose keys intersect the given time interval.
    # In this case, the entire layer will be read.
    gps.query(uri="file:///tmp/spacetime-catalog",
              layer_name="spacetime-layer",
              layer_zoom=7,
              time_intervals=[min_key.instant, max_key.instant])

.. code:: python3

    # It's possible to query a single time interval. By doing so, only Tiles that contain the time given will be
    # returned.
    gps.query(uri="file:///tmp/spacetime-catalog",
              layer_name="spacetime-layer",
              layer_zoom=7,
              time_intervals=[min_key.instant])

Querying by Space and Time
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

    # In addition to Polygons, one can also query using MultiPolygons.
    poly_1 = box(140.0, 60.0, 150.0, 65.0)
    poly_2 = box(160.0, 70.0, 179.0, 89.0)
    multi_poly = MultiPolygon(poly_1, poly_2)

.. code:: python3

    # Returns a TiledRasterLayer that contains the tiles which intersect the given polygons and are within the
    # specified time interval.
    gps.query(uri="file:///tmp/spacetime-catalog",
              layer_name="spacetime-layer",
              layer_zoom=7,
              query_geom=multi_poly,
              time_intervals=[min_key.instant, max_key.instant])

AttributeStore
--------------

When writing a layer, GeoTrellis uses an :class:`~geopyspark.geotrellis.catalog.AttributeStore` to write layer metadata required to read and query the layer later.
This class can be used outside of catalog ``write`` and ``query`` functions to inspect available layers and store additional, user defined, attributes.

Creating AttributeStore
~~~~~~~~~~~~~~~~~~~~~~~

``AttributeStore`` can be created from the same ``URI`` that is given to ``write`` and ``query`` functions.

.. code:: python3

   store = gps.AttributeStore(uri='file:///tmp/spatial-catalog')

   # Check if layer exists
   store.contains('spatial-layer', 11)

   # List layers stored in the catalog, giving list of AttributeStore.Attributes
   attributes_list = store.layers

   # Ask for layer attributes by name
   attributes = store.layer('spatial-layer', 11)

   # Read layer metadata
   attributes.layer_metadata()


User Defined Attributes
~~~~~~~~~~~~~~~~~~~~~~~

Internally :class:`~geopyspark.geotrellis.catalog.AttributeStore` is a key-value store where key is a tuple of layer name and zoom and values are encoded as JSON.
The layer metadata is stored under attribute named ``metadata``. Care should be taken to not overwrite this attribute.

.. code:: python3

   # Reading layer metadata as underlying JSON value
   attributes.read("metadata")

::

 {'header': {'format': 'file',
   'keyClass': 'geotrellis.spark.SpatialKey',
   'path': 'spatial-layer/11',
   'valueClass': 'geotrellis.raster.MultibandTile'},
  'keyIndex': {'properties': {'keyBounds': {'maxKey': {'col': 1485, 'row': 996}, 'minKey': {'col': 1479, 'row': 984}}},
   'type': 'zorder'},
  'metadata': {'bounds': {'maxKey': {'col': 1485, 'row': 996},
    'minKey': {'col': 1479, 'row': 984}},
   'cellType': 'int16',
   'crs': '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext +no_defs ',
   'extent': {'xmax': 9024345.159093022,
    'xmin': 8905559.263461886,
    'ymax': 781182.2141882492,
    'ymin': 542452.4856863784},
   'layoutDefinition': {'extent': {'xmax': 20037508.342789244,
     'xmin': -20037508.342789244,
     'ymax': 20037508.342789244,
     'ymin': -20037508.342789244},
    'tileLayout': {'layoutCols': 2048, 'layoutRows': 2048, 'tileCols': 256, 'tileRows': 256}}},
  'schema': {...}
 }


Otherwise you are free to store any additional attribute that is associated with the layer.
:class:`~geopyspark.geotrellis.catalog.AttributeStore.Attributes` provides ``write`` and ``read`` functions that accept and provide a dictionary.

.. code:: python3

   attributes.write("notes", {'a': 3, 'b': 5})
   notes_dict = attributes.read("notes)

A common use case for this is to store the layer histogram when writing a layer so it may be used for rendering later.

.. code:: python3

   # Calculate the histogram
   hist = spatial_tiled_layer.get_histogram()

   # GeoPySpark classes have to_dict as a convention when appropriate
   hist_dict = hist.to_dict()

   # Writing a dictionary that gets encoded as JSON
   attributes.write("histogram", hist_dict)

   # Reverse the process
   hist_read_dict = attributes.read("histogram")

   # GeoPySpark classes have from_dict static method as a convention
   hist_read = gps.Histogram.from_dict(hist_read_dict)

   # Use the histogram after round trip
   hist.min_max()


AttributeStore Caching
~~~~~~~~~~~~~~~~~~~~~~

An instance of :class:`~geopyspark.geotrellis.catalog.AttributeStore` keeps an in memory cache of attributes recently accessed.
This is done because a common access pattern to check layer existence, read the layer and decode the layer will produce repeated requests for layer metadata.
Depending on the backend used this may add considerable overhead and expense.

When writing a workflow that places heavy demand on :class:`~geopyspark.geotrellis.catalog.AttributeStore` reading it is worth while keeping track of a class instance and reusing it

.. code:: python3

   # Retrieve already created instance if its been asked for before
   store = gps.AttributeStore.cached(uri='file:///tmp/spatial-catalog')

   # Catalog functions have optional store parameter that allows its reuse
   gps.write(uri='file:///tmp/spatial-catalog',
          layer_name='spatial-layer',
          tiled_raster_layer=spatial_tiled_layer,
          store=store)

   gps.query(uri="file:///tmp/spatial-catalog",
          layer_name="spatial-layer",
          layer_zoom=11,
          store=store)

   gps.read_value(uri="file:///tmp/spatial-catalog",
          layer_name="spatial-layer",
          layer_zoom=11,
          col=min_key.col,
          row=min_key.row,
          store=store)
