
.. code:: ipython3

   import datetime
   import numpy as np
   import geopyspark as gps

Core Concepts
=============

Because GeoPySpark is a binding of an existing project,
`GeoTrellis <https://github.com/locationtech/geotrellis>`__, some
terminology and data representations have carried over. This section
seeks to explain this jargon in addition to describing how GeoTrellis
types are represented in GeoPySpark.

Rasters
-------

GeoPySpark differs in how it represents rasters from other geo-spatial
Python libraries like rasterIO. In GeoPySpark, they are represented by
the ``Tile`` class. This class contains a numpy array (refered to as
``cells``) that represents the cells of the raster in addition to other
information regarding the data. Along with ``cells``, ``Tile`` can also
have the ``no_data_value`` of the raster.

**Note**: All rasters in GeoPySpark are represented as having multiple
bands, even if the original raster just contained one.

.. code:: ipython3

    arr = np.array([[[0, 0, 0, 0],
                     [1, 1, 1, 1],
                     [2, 2, 2, 2]]], dtype=np.int16)
    
    # The resulting Tile will set -10 as the no_data_value for the raster
    gps.Tile.from_numpy_array(numpy_array=arr, no_data_value=-10)

.. code:: ipython3

    # The resulting Tile will have no no_data_value
    gps.Tile.from_numpy_array(numpy_array=arr)

Extent
------

Describes the area on Earth a raster represents. This area is
represented by coordinates that are in some Coordinate Reference System.
Thus, depending on the system in use, the values that outline the
``extent`` can vary. ``Extent`` can also be refered to as a *bounding
box*.

**Note**: The values within the ``Extent`` must be ``float``\ s and not
``double``\ s.

.. code:: ipython3

    extent = gps.Extent(0.0, 0.0, 10.0, 10.0)
    extent

ProjectedExtent
---------------

``ProjectedExtent`` describes both the area on Earth a raster represents
in addition to its CRS. Either the EPSG code or a proj4 string can be
used to indicate the CRS of the ``ProjectedExtent``.

.. code:: ipython3

    # Using an EPSG code
    
    gps.ProjectedExtent(extent=extent, epsg=3857)

.. code:: ipython3

    # Using a Proj4 String
    
    proj4 = "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs "
    gps.ProjectedExtent(extent=extent, proj4=proj4)

TemporalProjectedExtent
-----------------------

Similar to ``ProjectedExtent``, ``TemporalProjectedExtent`` describes
the area on Earth the raster represents, its CRS, and the time the data
was represents. This point of time, called ``instant``, is an instance
of ``datetime.datetime``.

.. code:: ipython3

    time = datetime.datetime.now()
    gps.TemporalProjectedExtent(extent=extent, instant=time, epsg=3857)

TileLayout
----------

``TileLayout`` describes the grid which represents how rasters are
orginized and assorted in a layer. ``layoutCols`` and ``layoutRows``
detail how many columns and rows the grid itself has, respectively.
While ``tileCols`` and ``tileRows`` tell how many columns and rows each
individual raster has.

.. code:: ipython3

    # Describes a layer where there are four rasters in a 2x2 grid. Each raster has 256 cols and rows.
    
    tile_layout = gps.TileLayout(layoutCols=2, layoutRows=2, tileCols=256, tileRows=256)
    tile_layout

LayoutDefinition
----------------

``LayoutDefinition`` describes both how the rasters are orginized in a
layer as well as the area covered by the grid.

.. code:: ipython3

    layout_definition = gps.LayoutDefinition(extent=extent, tileLayout=tile_layout)
    layout_definition

Tiling Strategies
-----------------

It is often the case that the exact layout of the layer is unknown.
Rather than having to go through the effort of trying to figure out the
optimal layout, there exists two different tiling strategies that will
produce a layout based on the data they are given.

LocalLayout
~~~~~~~~~~~

``LocalLayout`` is the first tiling strategy that produces a layout
where the grid is constructed over all of the pixels within a layer of a
given tile size. The resulting layout will match the original resolution
of the cells within the rasters.

**Note**: This layout **cannot be used for creating display layers.
Rather, it is best used for layers where operations and analysis will be
performed.**

.. code:: ipython3

    # Creates a LocalLayout where each tile within the grid will be 256x256 pixels.
    gps.LocalLayout()

.. code:: ipython3

    # Creates a LocalLayout where each tile within the grid will be 512x512 pixels.
    gps.LocalLayout(tile_size=512)

.. code:: ipython3

    # Creates a LocalLayout where each tile within the grid will be 256x512 pixels.
    gps.LocalLayout(tile_cols=256, tile_rows=512)

GlobalLayout
~~~~~~~~~~~~

The other tiling strategy is ``GlobalLayout`` which makes a layout where
the grid is constructed over the global extent CRS. The cell resolution
of the resulting layer be multiplied by a power of 2 for the CRS. Thus,
using this strategy will result in either up or down sampling of the
original raster.

**Note**: This layout strategy **should be used when the resulting layer
is to be dispalyed in a TMS server.**

.. code:: ipython3

    # Creates a GobalLayout instance with the default values
    gps.GlobalLayout()

.. code:: ipython3

    # Creates a GlobalLayout instance for a zoom of 12
    gps.GlobalLayout(zoom=12)

You may have noticed from the above two examples that ``GlobalLayout``
does not create layout for a given zoom level by default. Rather, it
determines what the zoom should be based on the size of the cells within
the rasters. If you do want to create a layout for a specific zoom
level, then the ``zoom`` parameter must be set.

SpatialKey
----------

``SpatialKey``\ s describe the positions of rasters within the grid of
the layout. This grid is a 2D plane where the location of a raster is
represented by a pair of coordinates, ``col`` and ``row``, respectively.
As its name and attributes suggest, ``SpatialKey`` deals solely with
spatial data.

.. code:: ipython3

    gps.SpatialKey(col=0, row=0)

SpaceTimeKey
------------

Like ``SpatialKey``\ s, ``SpaceTimeKey``\ s describe the position of a
raster in a layout. However, the grid is a 3D plane where a location of
a raster is represented by a pair of coordinates, ``col`` and ``row``,
as well as a z value that represents a point in time called,
``instant``. Like the ``instant`` in ``TemporalProjectedExtent``, this
is also an instance of ``datetime.datetime``. Thus, ``SpaceTimeKey``\ s
deal with spatial-temporal data.

.. code:: ipython3

    gps.SpaceTimeKey(col=0, row=0, instant=time)

Bounds
------

``Bounds`` represents the the extent of the layout grid in terms of
keys. It has both a ``minKey`` and a ``maxKey`` attributes. These can
either be a ``SpatialKey`` or a ``SpaceTimeKey`` depending on the type
of data within the layer. The ``minKey`` is the left, uppermost cell in
the grid and the ``maxKey`` is the right, bottommost cell.

.. code:: ipython3

    # Creating a Bounds from SpatialKeys
    
    min_spatial_key = gps.SpatialKey(0, 0)
    max_spatial_key = gps.SpatialKey(10, 10)
    
    bounds = gps.Bounds(min_spatial_key, max_spatial_key)
    bounds

.. code:: ipython3

    # Creating a Bounds from SpaceTimeKeys
    
    min_space_time_key = gps.SpaceTimeKey(0, 0, 1.0)
    max_space_time_key = gps.SpaceTimeKey(10, 10, 1.0)
    
    gps.Bounds(min_space_time_key, max_space_time_key)

Metadata
--------

``Metadata`` contains information of the values within a layer. This
data pertains to the layout, projection, and extent of the data
contained within the layer.

The below example shows how to construct ``Metadata`` by hand, however,
this is almost never required and ``Metadata`` can be produced using
easier means. For ``RasterLayer``, one call the method,
``collect_metadata()`` and ``TiledRasterLayer`` has the attribute,
``layer_metadata``.

.. code:: ipython3

    # Creates Metadata for a layer with rasters that have a cell type of int16 with the previously defined
    # bounds, crs, extent, and layout definition.
    gps.Metadata(bounds=bounds,
                 crs=proj4,
                 cell_type=gps.CellType.INT16.value,
                 extent=extent,
                 layout_definition=layout_definition)
