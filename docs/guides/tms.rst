TMS Servers
===========

GeoPySpark is meant to work with geospatial data.  The most natural way to
interact with these data is to display them on a map.  In order to allow for
this interactive visualization, we provide a means to create Tile Map Service
(TMS) servers directly from both GeoPySpark RDDs and tile catalogs.  A TMS
server may be viewed using a web-based tool such as geojson.io_ or interacted
with using the GeoNotebook_ Jupyter kernel. [#]_

Note that the following examples rely on this common boilerplate code:

.. code-block:: python

   import geopyspark as gps
   from pyspark import SparkContext

   conf = gps.geopyspark_conf(appName="demo")
   sc = SparkContext(conf=conf)

Basic Example
-------------

The most straightforward use case of the TMS server is to display a singleband
layer with some custom color map.  This is accomplished easily:

.. code-block:: python

   cm = gps.ColorMap.nlcd_colormap()

   tms = gps.TMS.build(('s3://azavea-datahub/catalog', 'nlcd-tms-epsg3857'), display=cm)

Of course, other color maps can be used.  See the documentation for
:class:`~geopyspark.geotrellis.color.ColorMap` for more details.
   
:code:`TMS.build` can display data from catalogs—which are represented as a
string-string pair containing the URI of the catalog root and the name of the
layer—or from a :class:`~geopyspark.geotrellis.layer.Pyramid` object.  One may also
specify a list of any combination of these sources; more on multiple sources
below.

Once a TMS server is constructed, we need to make the contents visible by
binding the server.  The :meth:`~geopyspark.geotrellis.tms.bind` method can
take a ``host`` and/or a ``port``, where the former is a string, and the
latter is an integer.  Providing neither will result in a TMS server
accessible from localhost on a random port.  If the server should be
accessible from the outside world, a ``host`` value of ``"0.0.0.0"`` may be
used.

A call to :meth:`~geopyspark.geotrellis.tms.bind` is then followed by a call
to :meth:`~geopyspark.geotrellis.tms.url_pattern`, which provides a string
that gives the template for the tiles furnished by the TMS server.  This
template string may be copied directly into geojson.io_, for example.  When
the TMS server is no longer needed, its resources can be freed by a call to
:meth:`~geopyspark.geotrellis.tms.unbind`.

.. code-block:: python

   # set up the TMS server to serve from 'localhost' on a random port
   tms.bind()

   tms.url_pattern

   # (browse the the TMS-served layer in some interface)

   tms.unbind()

In the event that one is using GeoPySpark from within the GeoNotebook
environment, ``bind`` should not be used, and the following code should be
used instead:

.. code-block:: python

   from geonotebook.wrappers import TMSRasterData
   M.add_layer(TMSRasterData(tms), name="NLCD")

Custom Rendering Functions
--------------------------

For the cases when more than a simple color map needs to be applied, one may
also specify a custom rendering function. [#]_  There are two methods for
custom rendering depending on whether one is rendering a single layer or
compositing multiple layers.  We address each in turn.

Rendering Single Layers
^^^^^^^^^^^^^^^^^^^^^^^

If one has special demands for display—including possible ad-hoc
manipulation of layer data during the display process—then one may write a
Python function to convert some tile data into an image that may be served via
the TMS server.

The general approach is to develop a function taking a
:class:`~geopyspark.geotrellis.Tile` that returns a byte array containing the
resulting image, encoded as PNG or JPG.  The following example uses this
rendering function approach to apply the same simple color map as above.

.. code-block:: python

   from PIL import Image
   import numpy as np

   def hex_to_rgb(value):
      """Return (red, green, blue) for the color given as #rrggbb."""
      value = value.lstrip('#')
      lv = len(value)
      return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))

   nlcd_color_map =  { 0  : "#00000000",
                       11 : "#526095FF",     # Open Water
                       12 : "#FFFFFFFF",     # Perennial Ice/Snow
                       21 : "#D28170FF",     # Low Intensity Residential
                       22 : "#EE0006FF",     # High Intensity Residential
                       23 : "#990009FF",     # Commercial/Industrial/Transportation
                       31 : "#BFB8B1FF",     # Bare Rock/Sand/Clay
                       32 : "#969798FF",     # Quarries/Strip Mines/Gravel Pits
                       33 : "#382959FF",     # Transitional
                       41 : "#579D57FF",     # Deciduous Forest
                       42 : "#2A6B3DFF",     # Evergreen Forest
                       43 : "#A6BF7BFF",     # Mixed Forest
                       51 : "#BAA65CFF",     # Shrubland
                       61 : "#45511FFF",     # Orchards/Vineyards/Other
                       71 : "#D0CFAAFF",     # Grasslands/Herbaceous
                       81 : "#CCC82FFF",     # Pasture/Hay
                       82 : "#9D5D1DFF",     # Row Crops
                       83 : "#CD9747FF",     # Small Grains
                       84 : "#A7AB9FFF",     # Fallow
                       85 : "#E68A2AFF",     # Urban/Recreational Grasses
                       91 : "#B6D8F5FF",     # Woody Wetlands
                       92 : "#B6D8F5FF" }    # Emergent Herbaceous Wetlands

   def rgba_functions(color_map):
      m = {}
      for key in color_map:
         m[key] = hex_to_rgb(color_map[key])


      def r(v):
         if v in m:
            return m[v][0]
         else:
            return 0

      def g(v):
         if v in m:
            return m[v][1]
         else:
            return 0

      def b(v):
         if v in m:
            return m[v][2]
         else:
            return 0

      def a(v):
         if v in m:
            return m[v][3]
         else:
            return 0x00

      return (np.vectorize(r), np.vectorize(g), np.vectorize(b), np.vectorize(a))

   def render_nlcd(tile):
      '''
      Assumes that the tile is a multiband tile with a single band.
      (meaning shape = (1, cols, rows))
      '''
      arr = tile.cells[0]
      (r, g, b, a) = rgba_functions(nlcd_color_map)

      rgba = np.dstack([r(arr), g(arr), b(arr), a(arr)]).astype('uint8')

      img = Image.fromarray(rgba, mode='RGBA')

      return img

   tms = gps.TMS.build(('s3://azavea-datahub/catalog', 'nlcd-tms-epsg3857'), display=render_nlcd)

You will likely observe noticeably slower performance compared to the earlier
example.  This is because the contents of each tile must be transferred from
the JVM to the Python environment prior to rendering.  If performance is
important to you, and a color mapping solution is available, please use that
approach.


Compositing Multiple Layers
^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is also possible to combine data from various sources at the time of
display.  Of course, one could use map algebra to produce a composite layer,
but if the input layers are large, this could potentially be a time-consuming
operation.  The TMS server allows for a list of sources to be supplied; these
may be any combination of :class:`~geopyspark.geotrellis.layer.Pyramid`
objects and catalogs.  We then may supply a function that takes a list of
:class:`~geopyspark.geotrellis.Tile` instances and produces the bytes of an
image as in the single-layer case.

The following example masks the NLCD layer to areas above 1371 meters, using
some of the helper functions from the previous example.

.. code-block:: python
                
   from scipy.interpolate import interp2d

   def comp(tiles):
      elev256 = tiles[0].cells[0]
      grid256 = range(256)
      f = interp2d(grid256, grid256, elev256)
      grid512 = np.arange(0, 256, 0.5)
      elev = f(grid512, grid512)

      land_use = tiles[1].cells[0]
    
      arr = land_use
      arr[elev < 1371] = 0

      (r, g, b, a) = rgba_functions(nlcd_color_map)

      rgba = np.dstack([r(arr), g(arr), b(arr), a(arr)]).astype('uint8')

      img = Image.fromarray(rgba, mode='RGBA')

      return img
    
   tms = gps.TMS.build([
      ('s3://azavea-datahub/catalog', 'us-ned-tms-epsg3857'),
      ('s3://azavea-datahub/catalog', 'nlcd-tms-epsg3857')],
      display=comp)

This example shows the major pitfall likely to be encountered in this
approach: tiles of different size must be somehow combined.  NLCD tiles are
512x512, while the National Elevation Data (NED) tiles are 256x256.  In this
example, the NED data is (bilinearly) resampled using scipy's ``interp2d``
function to the proper size.

Debugging Considerations
^^^^^^^^^^^^^^^^^^^^^^^^

Be aware that if there are problems in the rendering or compositing functions,
the TMS server will tend to produce empty images, which can result in a silent
failure of a layer to display, or odd exceptions in programs expecting
meaningful images, such as GeoNotebook.  It is advisable to thoroughly test
these rendering functions ahead of deployment, as errors encountered in their
use will be largely invisible.

.. _geojson.io: http://geojson.io
.. _GeoNotebook: https://github.com/OpenGeoscience/geonotebook
.. [#] Note that changes allowing for display of TMS-served tiles in
       GeoNotebook have not yet been accepted into the master branch of that
       repository.  In the meantime, find a TMS-enabled fork at
       http://github.com/geotrellis/geonotebook.
.. [#] If one is only applying a colormap to a singleband tile layer, a custom
       rendering function should not be used as it will be noticeably slower
       to display.
