Map Algebra
===========

Given a set of raster layers, it may be desirable to combine and filter
the content of those layers. This is the function of *map algebra*. Two
classes of map algebra operations are provided by GeoPySpark: *local*
and *focal* operations. Local operations individually consider the
pixels or cells of one or more rasters, applying a function to the
corresponding cell values. For example, adding two rasters' pixel values
to form a new layer is a local operation.

Focal operations consider a region around each pixel of an input raster
and apply an operation to each region. The result of that operation is
stored in the corresponding pixel of the output raster. For example, one
might weight a 5x5 region centered at a pixel according to a 2d Gaussian
to effect a blurring of the input raster. One might consider this
roughly equivalent to a 2d convolution operation.

**Note:** Map algebra operations work only on ``TiledRasterLayer``\ s,
and if a local operation requires multiple inputs, those inputs must
have the same layout and projection.

Before begining, all examples in this guide need the following boilerplate
code:

.. code:: python3

   import geopyspark as gps
   import numpy as np

   from pyspark import SparkContext
   from shapely.geometry import Point, MultiPolygon, LineString, box

   conf = gps.geopyspark_conf(master="local[*]", appName="map-algebra")
   pysc = SparkContext(conf=conf)

   # Setting up the data

   cells = np.array([[[3, 4, 1, 1, 1],
                      [7, 4, 0, 1, 0],
                      [3, 3, 7, 7, 1],
                      [0, 7, 2, 0, 0],
                      [6, 6, 6, 5, 5]]], dtype='int32')

   extent = gps.ProjectedExtent(extent = gps.Extent(0, 0, 5, 5), epsg=4326)

   layer = [(extent, gps.Tile.from_numpy_array(numpy_array=cells))]

   rdd = pysc.parallelize(layer)
   raster_layer = gps.RasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, rdd)
   tiled_layer = raster_layer.tile_to_layout(layout=gps.LocalLayout(tile_size=5))

Local Operations
----------------

Local operations on ``TiledRasterLayer``\ s can use ``int``\ s,
``float``\ s, or other ``TiledRasterLayer``\ s. ``+``, ``-``, ``*``,
``/``, ``**``, and ``abs`` are all of the local operations that currently supported.

.. code:: python3

    (tiled_layer + 1)

    (2 - (tiled_layer * 3))

    ((tiled_layer + tiled_layer) / (tiled_layer + 1))

    abs(tiled_layer)

    2 ** tiled_layer

A :class:`~geopyspark.geotrellis.layer.Pyramid` can also be used in local
operations. The types that can be used in local operations with
``Pyramid``\ s are: ``int``\ s, ``float``\ s, ``TiledRasterLayer``\ s,
and other ``Pyramid``\ s.

**Note**: Like with ``TiledRasterLayer``, performing calculations on
multiple ``Pyramid``\ s or ``TiledRasterLayer``\ s means they must all
have the same layout and projection.

.. code:: python3

    # Creating out Pyramid
    pyramid = tiled_layer.pyramid()

    pyramid + 1

    (pyramid - tiled_layer) * 2

Focal Operations
----------------

Focal operations are performed in GeoPySpark by executing a given
operation on a neighborhood throughout each tile in the layer. One can
select a neighborhood to use from the ``Neighborhood`` enum class.
Likewise, an operation can be choosen from the enum class,
``Operation``.

.. code:: python3

    # This creates an instance of Square with an extent of 1. This means that
    # each operation will be performed on a 3x3
    # neighborhood.

    '''
    A square neighborhood with an extent of 1.
    o = source cell
    x = cells that fall within the neighbhorhood

    x x x
    x o x
    x x x
    '''

    square = gps.Square(extent=1)

Mean
^^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.MEAN, neighborhood=square)

Median
^^^^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.MEDIAN, neighborhood=square)

Mode
^^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.MODE, neighborhood=square)

Sum
^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.SUM, neighborhood=square)

Standard Deviation
^^^^^^^^^^^^^^^^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.STANDARD_DEVIATION, neighborhood=square)

Min
^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.MIN, neighborhood=square)

Max
^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.MAX, neighborhood=square)

Slope
^^^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.SLOPE, neighborhood=square)

Aspect
^^^^^^

.. code:: python3

    tiled_layer.focal(operation=gps.Operation.ASPECT, neighborhood=square)

Miscellaneous Raster Operations
--------------------------------

There are other means to extract information from rasters and to create
rasters that need to be presented. These are *polygonal summaries*,
*cost distance*, and *rasterization*.

Polygonal Summary Methods
^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to local and focal operations, polygonal summaries can also
be performed on ``TiledRasterLayer``\ s. These are operations that are
executed in the areas that intersect a given geometry and the layer.

**Note**: It is important the given geometry is in the same projection
as the layer. If they are not, then either incorrect and/or only partial
results will be returned.

.. code:: python3

    tiled_layer.layer_metadata

Polygonal Min
~~~~~~~~~~~~~

.. code:: python3

    poly_min = box(0.0, 0.0, 1.0, 1.0)
    tiled_layer.polygonal_min(geometry=poly_min, data_type=int)

Polygonal Max
~~~~~~~~~~~~~

.. code:: python3

    poly_max = box(1.0, 0.0, 2.0, 2.5)
    tiled_layer.polygonal_min(geometry=poly_max, data_type=int)

Polygonal Sum
~~~~~~~~~~~~~

.. code:: python3

    poly_sum = box(0.0, 0.0, 1.0, 1.0)
    tiled_layer.polygonal_min(geometry=poly_sum, data_type=int)

Polygonal Mean
~~~~~~~~~~~~~~

.. code:: python3

    poly_max = box(1.0, 0.0, 2.0, 2.0)
    tiled_layer.polygonal_min(geometry=poly_max, data_type=int)

Cost Distance
^^^^^^^^^^^^^^

:meth:`~geopyspark.geotrellis.cost_distance.cost_distance` is an iterative
method for approximating the weighted distance from a raster cell to a given
geometry. The ``cost_distance`` function takes in a geometry and a
“friction layer” which essentially describes how difficult it is to traverse
each raster cell. Cells that fall within the geometry have a final cost of
zero, while friction cells that contain noData values will correspond to
noData values in the final result. All other cells have a value that describes
the minimum cost of traversing from that cell to the geometry. If the friction
layer is uniform, this function approximates the Euclidean distance, modulo some
scalar value.

.. code:: python3

    cost_distance_cells = np.array([[[1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 1.0],
                                     [1.0, 1.0, 1.0, 1.0, 0.0]]])

    tile = gps.Tile.from_numpy_array(numpy_array=cost_distance_cells, no_data_value=-1.0)
    cost_distance_extent = gps.ProjectedExtent(extent=gps.Extent(xmin=0.0, ymin=0.0, xmax=5.0, ymax=5.0), epsg=4326)
    cost_distance_layer = [(cost_distance_extent, tile)]

    cost_distance_rdd = pysc.parallelize(cost_distance_layer)
    cost_distance_raster_layer = gps.RasterLayer.from_numpy_rdd(gps.LayerType.SPATIAL, cost_distance_rdd)
    cost_distance_tiled_layer = cost_distance_raster_layer.tile_to_layout(layout=gps.LocalLayout(tile_size=5))

    gps.cost_distance(friction_layer=cost_distance_tiled_layer, geometries=[Point(0.0, 5.0)], max_distance=144000.0)

Rasterization
^^^^^^^^^^^^^^

It may be desirable to convert vector data into a raster layer. For
this, we provide the :meth:`~geopyspark.geotrellis.rasterize.rasterize`
function, which determines the set of pixel values covered by each vector
element, and assigns a supplied value to that set of pixels in a target raster.
If, for example, one had a set of polygons representing counties in the US, and
a value for, say, the median income within each county, a raster could be made
representing these data.

GeoPySpark's ``rasterize`` function takes a list of any number of
Shapely geometries, converts them to rasters, tiles the rasters to a
given layout, and then produces a ``TiledRasterLayer`` with these tiled
values.

Rasterize MultiPolygons
~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python3

    raster_poly_1 = box(0.0, 0.0, 5.0, 10.0)
    raster_poly_2 = box(3.0, 6.0, 15.0, 20.0)
    raster_poly_3 = box(13.5, 17.0, 30.0, 20.0)

    raster_multi_poly = MultiPolygon([raster_poly_1, raster_poly_2, raster_poly_3])

.. code:: python3

    # Creates a TiledRasterLayer that contains the MultiPolygon with a CRS of EPSG:3857 at zoom level 5.
    gps.rasterize(geoms=[raster_multi_poly], crs=4326, zoom=5, fill_value=1)

Rasterize LineStrings
~~~~~~~~~~~~~~~~~~~~~

.. code:: python3

    line_1 = LineString(((0.0, 0.0), (0.0, 5.0)))
    line_2 = LineString(((7.0, 5.0), (9.0, 12.0), (12.5, 15.0)))
    line_3 = LineString(((12.0, 13.0), (14.5, 20.0)))

.. code:: python3

    # Creates a TiledRasterLayer whose cells have a data type of int16.
    gps.rasterize(geoms=[line_1, line_2, line_3], crs=4326, zoom=3, fill_value=2, cell_type=gps.CellType.INT16)

Rasterize Polygons and LineStrings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python3

    # Creates a TiledRasterLayer with both the LineStrings and the MultiPolygon
    gps.rasterize(geoms=[line_1, line_2, line_3, raster_multi_poly], crs=4326, zoom=5, fill_value=2)
