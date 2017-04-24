Creating a Tile Server From Ingested, Sentinel Data
****************************************************

Now that we have ingested data, we can use it using a tile server.
We will be using the catalog that was created in :ref:`sentinel_ingest_example`.

**Note**: It is assumed that the previously mentioned script was ran and
completed successfully.

**Note**: This guide will focus on converting the raster into a ``PIL`` RGB
image so that it can be used by the tile server. The tile server process itself
is discussed in more detail in :ref:`server_break_down`.

The Code
=========

Here is the code itself. We will be using ``flask`` to create a local server
and ``PIL`` to create our images. Because we are working with a RGB, mulitband
image, we will need to correct the colors for each tile in order for it to
displayed correctly.

.. code:: python

  import io
  import numpy as np
  import rasterio

  from flask import Flask, make_response
  from PIL import Image

  from geopyspark.geopycontext import GeoPyContext
  from geopyspark.geotrellis.catalog import read_value
  from geopyspark.geotrellis.constants import SPATIAL


  # normalize the data so that it falls in the range of 0 - 255
  def make_image(arr):
      adjusted = ((arr - whole_min) * 255) / (whole_max - whole_min)
      return Image.fromarray(adjusted.astype('uint8')).resize((256, 256), Image.NEAREST).convert('L')


  app = Flask(__name__)

  @app.route("/<int:zoom>/<int:x>/<int:y>.png")
  def tile(x, y, zoom):
      # fetch tile

      tile = read_value(geopysc, SPATIAL, uri, layer_name, zoom, x, y)
      arr = tile['data']

      bands = arr.shape[0]
      arrs = [np.array(arr[x, :, :]).reshape(256, 256) for x in range(bands)]

      # display tile
      images = [make_image(arr) for arr in arrs]
      image = Image.merge('RGB', images)

      bio = io.BytesIO()
      image.save(bio, 'PNG')
      response = make_response(bio.getvalue())
      response.headers['Content-Type'] = 'image/png'
      response.headers['Content-Disposition'] = 'filename=%d.png' % 0

      return response


  if __name__ == "__main__":
      uri = "file:///tmp/sentinel-catalog"
      layer_name = "sentinel-example"

      geopysc = GeoPyContext(appName="s3-flask", master="local[*]")

      with open('/tmp/sentinel_stats.txt', 'r') as f:
          lines = f.readlines()
          whole_max = int(lines[0])
          whole_min = int(lines[1])

      app.run()

Running the Code
-----------------

You will want to run this code through the command line. To run it, from the
file, go to the directory the file is in and run this command

.. code-block:: none

  python3 file.py

Just replace ``file.py`` with whatever name you decided to call the file.

Once it's started, you'll then want to go to a website that allows you to
display geo-spatial images from a server. For this example, we'll be using
`geojson.io <http://geojson.io>`_, but feel free to use whatever service you
want.

.. image:: pictures/geojson.png
   :align: center

Go to geojson.io, and select the ``Meta`` option from the tool bar, and then
choose the ``Add map layer`` command.

.. image:: pictures/toolbar.png
   :align: center

A pop up will appear where it will ask for the template, layer URL. To get this example to work,
please enter the following: ``http://localhost:5000/{z}/{x}/{y}.png``.

.. image:: pictures/address.png
   :align: center

A second window will appear asking to name the new layer. Pick whatever you want.
I tend to use simple names like ``a``, ``b``, ``c``, etc.

.. image:: pictures/sentinel_image.png
   :align: center

Now that everything is setup, it's time to see the image. You'll need to scroll
over Corsica, and you should see something that matches the above image. If you
do, then the server works!


Breaking Down the Code
=======================

As with our other examples, let's go through it step-by-step to see what's
actually going on. Though, for this example, we'll be starting at the bottom
and working our way up.

**Note**: This next section will go over how to prepare the RGB image to be
served. For a more of a general overview of to setup a tile server please see
:ref:`server_break_down`.

Setup
------

.. code-block:: python

  if __name__ == "__main__":
      uri = "file:///tmp/sentinel-catalog"
      layer_name = "sentinel-example"

      geopysc = GeoPyContext(appName="s3-flask", master="local[*]")

      with open('/tmp/sentinel_stats.txt', 'r') as f:
          lines = f.readlines()
          whole_max = int(lines[0])
          whole_min = int(lines[1])

      app.run()

In additon to setting up ``uri`` and ``layer_name``, we will also read in the
``max`` and ``min`` values that we saved earlier. These will be used when we
normalize the tile that is to be served.


Preparing the Tile
------------------

.. code-block:: python

  # normalize the data so that it falls in the range of 0 - 255
  def make_image(arr):
      adjusted = ((arr - whole_min) * 255) / (whole_max - whole_min)
      return Image.fromarray(adjusted.astype('uint8')).resize((256, 256), Image.NEAREST).convert('L')

  app = Flask(__name__)

  @app.route("/<int:zoom>/<int:x>/<int:y>.png")
  def tile(x, y, zoom):
      # fetch tile

      tile = read_value(geopysc, SPATIAL, uri, layer_name, zoom, x, y)
      arr = tile['data']

      bands = arr.shape[0]
      arrs = [np.array(arr[x, :, :]).reshape(256, 256) for x in range(bands)]

      # display tile
      images = [make_image(arr) for arr in arrs]
      image = Image.merge('RGB', images)


Tiles that contains mulitbands need some work done before they can be served.
The ``make_image`` method takes each band and normalizes it between a range
of 0 and 255. We need to do this because ``PIL`` expects the data types of
arrays to be ``uint8``. This is why we need the ``whole_max`` and the
``whole_min``. As it is impossilbe to get this information any other way at
this point.

Once normalized, the band is then converted to a greyscale image. This is done
for each band in the tile, and once complete, we can then make a RGB ``png``
file. After this step, the remaining process is no different than if you were
working with a singleband tile.

Any details that we not dicussed in this document can be found in :ref:`server_break_down`.
