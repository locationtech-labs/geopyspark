.. _visualizing:

Visualizing Data in GeoPySpark
==============================

Data is visualized in GeoPySpark by running a server which allows it to
be viewed in an interactive way. Before putting the data on the server,
however, it must first be formatted and colored. This guide seeks to go
over the steps needed to create a visualization server in GeoPySpark.

Before begining, all examples in this guide need the following boilerplate
code:

.. code:: python3

    !curl -o /tmp/cropped.tif https://s3.amazonaws.com/geopyspark-test/example-files/cropped.tif

.. code:: python3

    import geopyspark as gps
    import matplotlib.pyplot as plt

    from colortools import Color
    from pyspark import SparkContext

    %matplotlib inline

    conf = gps.geopyspark_conf(master="local[*]", appName="visualization")
    pysc = SparkContext(conf=conf)

    raster_layer = gps.geotiff.get(layer_type=gps.LayerType.SPATIAL, uri="/tmp/cropped.tif")
    tiled_layer = raster_layer.tile_to_layout(layout=gps.GlobalLayout(), target_crs=3857)


.. _pyramid:

Pyramid
-------

The :class:`~geopyspark.Pyramid` class represents a list of ``TiledRasterLayer``\ s
that represent the same area where each layer is a level within the pyramid
at a specific zoom level. Thus, as one moves up the pyramid (starting a
level 0), the image will have its pixel resolution increased by a power of 2
for each level. It is this varying level of detail that allows an
interactive tile server to be created from a ``Pyramid``. This class is
needed in order to create visualizations of the contents within its layers.

Creating a Pyramid
~~~~~~~~~~~~~~~~~~

There are currently two different ways to create a ``Pyramid`` instance:
Through the ``TiledRasterLayer.pyramid`` method or by constructing it by
passing in a ``[TiledRasterLayer]`` or
``{zoom_level: TiledRasterLayer}`` to ``Pyramid``.

Any ``TiledRasterLayer`` with a ``max_zoom`` can be pyramided. However,
the resulting ``Pyramid`` may have limited functionality depending on
the layout of the source ``TiledRasterLayer``. In order to be used for
visualization, the ``Pyramid`` **must** have been created from
``TiledRasterLayer`` that was tiled using a ``GlobalLayout`` and whose
tiles have a spatial resolution of a power of 2.

Via the pyramid Method
^^^^^^^^^^^^^^^^^^^^^^

When using the ``Pyramid`` method, a ``Pyramid`` instance will be
created with levels from 0 to ``TiledRasterlayer.zoom_level``. Thus, if
a ``TiledRasterLayer`` has a ``zoom_level`` of 12 then the resulting
``Pyramid`` will have 13 levels that each correspond to a zoom from 0 to
12.

.. code:: python3

    pyramided = tiled_layer.pyramid()

Contrusting a Pyramid Manually
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

    gps.Pyramid([tiled_layer.tile_to_layout(gps.GlobalLayout(zoom=x)) for x in range(0, 13)])

.. code:: python3

    gps.Pyramid({x: tiled_layer.tile_to_layout(gps.GlobalLayout(zoom=x)) for x in range(0, 13)})

Computing the Histogram of a Pyramid
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can produce a ``Histogram`` instance representing the bottom most layer
within a ``Pyramid`` via the :meth:`~geopyspark.Pyramid.get_histogram` method.

.. code:: python3

    hist = pyramided.get_histogram()
    hist

RDD Methods
~~~~~~~~~~~

``Pyramid`` contains methods for working with the ``RDD``\ s contained
within its ``TiledRasterLayer``\ s. A list of these can be found
here :ref:`rdd-methods`. When used, all internal ``RDD``\ s
will be operated on.

Map Algebra
~~~~~~~~~~~

While not as versatile as ``TiledRasterLayer`` in terms of map algebra
operations, ``Pyramid``\ s are still able to perform local operations
between themselves, ``int``\ s, and ``float``\ s.

**Note**: Operations between two or more ``Pyramid``\ s will occur on a
per ``Tile`` basis which depends on the tiles having the same key. It is
therefore possible to do an operation between two ``Pyramid``\ s and
getting a result where nothing has changed if neither of the
``Pyramid``\ s have matching keys.

.. code:: python3

    pyramided + 1

    (2 * (pyramided + 2)) / 3

When performing operations on two or more ``Pyramid``\ s, if the
``Pyamid``\ s involved have different number of ``level``\ s, then the
resulting ``Pyramid`` will only have as many levels as the source
``Pyramid`` with the smallest level count.

.. code:: python3

    small_pyramid = gps.Pyramid({x: tiled_layer.tile_to_layout(gps.GlobalLayout(zoom=x)) for x in range(0, 5)})
    result = pyramided + small_pyramid
    result.levels

ColorMap
--------

The :class:`~geopyspark.ColorMap` class in GeoPySpark acts as a wrapper for the
GeoTrellis ``ColorMap`` class. It is used to colorize the data within a
layer when it's being visualized.

Constructing a Color Ramp
~~~~~~~~~~~~~~~~~~~~~~~~~

Before we can initialize ``ColorMap`` we must first create a list of
colors (or a color ramp) to pass in. This can be created either through
a function in the ``color`` module or manually.

Using Matplotlib
^^^^^^^^^^^^^^^^

The ``get_colors_from_matplotlib`` function creates a color ramp using
the name of on an existing in color ramp in `Matplotlib <https://matplotlib.org>`_
and the number of colors.

**Note**: This function will not work if ``Matplotlib`` is not
installed.

.. code:: python3

    gps.get_colors_from_matplotlib(ramp_name="viridis")

.. code:: python3

    gps.get_colors_from_matplotlib(ramp_name="hot", num_colors=150)

From ColorTools
^^^^^^^^^^^^^^^

The second helper function for constructing a color ramp is
``get_colors_from_colors``. This uses the `colortools <https://pypi.python.org/pypi/colortools/0.1.2>`_
package to build the ramp from ``[Color]`` instances.

**Note**: This function will not work if ``colortools`` is not
installed.

.. code:: python3

    colors = [Color('green'), Color('red'), Color('blue')]
    colors

.. code:: python3

    colors_color_ramp = gps.get_colors_from_colors(colors=colors)
    colors_color_ramp

Creating a ColorMap
~~~~~~~~~~~~~~~~~~~

``ColorMap`` has many different ways of being constructed depending on
the inputs it's given.

From a Histogram
^^^^^^^^^^^^^^^^

.. code:: python3

    gps.ColorMap.from_histogram(histogram=hist, color_list=colors_color_ramp)

From a List of Colors
^^^^^^^^^^^^^^^^^^^^^

.. code:: python3

    # Creates a ColorMap instance that will have three colors for the values that are less than or equal to 0, 250, and
    # 1000.
    gps.ColorMap.from_colors(breaks=[0, 250, 1000], color_list=colors_color_ramp)

For NLCD Data
^^^^^^^^^^^^^

If the layers you are working with contain data from NLCD, then it is
possible to construct a ``ColorMap`` without first making a color ramp
and passing in a list of breaks.

.. code:: python3

    gps.ColorMap.nlcd_colormap()

From a Break Map
^^^^^^^^^^^^^^^^

If there aren't many colors to work with in the layer, than it may be
easier to construct a ``ColorMap`` using a ``break_map``, a ``dict``
that maps tile values to colors.

.. code:: python3

    # The three tile values are 1, 2, and 3 and they correspond to the colors 0x00000000, 0x00000001, and 0x00000002
    # respectively.
    break_map = {
        1: 0x00000000,
        2: 0x00000001,
        3: 0x00000002
    }

    gps.ColorMap.from_break_map(break_map=break_map)

More General Build Method
^^^^^^^^^^^^^^^^^^^^^^^^^

As mentioned above, ``ColorMap`` has a more general ``classmethod``
called :meth:`~geopyspark.ColorMap.build` which takes a wide range of types to
construct a ``ColorMap``. In the following example, ``build`` will be passed the
same inputs used in the previous examples.

.. code:: python3

    # build using a Histogram
    gps.ColorMap.build(breaks=hist, colors=colors_color_ramp)

    # It is also possible to pass in the name of Matplotlib color ramp instead of constructing it yourself
    gps.ColorMap.build(breaks=hist, colors="viridis")

    # build using Colors
    gps.ColorMap.build(breaks=colors_color_ramp, colors=colors)

    # buld using breaks
    gps.ColorMap.build(breaks=break_map)

Additional Coloring Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to supplying breaks and color values to ``ColorMap``, there
are other ways of changing the coloring strategy of a layer.

The following additional parameters that can be changed:

-  ``no_data_color``: The color of the ``no_data_value`` of the
   ``Tile``\ s. The default is ``0x00000000``
-  ``fallback``: The color to use when a ``Tile`` value has no color
   mapping. The default is ``0x00000000``
-  ``classification_strategy``: How the colors should be assigned to the
   values based on the breaks. The default is
   ``ClassificationStrategy.LESS_THAN_OR_EQUAL_TO``.
