.. _sentinel_ingest_example:

Ingesting a Sentinel Image Using GeoPySpark
********************************************

Sentinel-2 is an observation mission developed by the European Space Agency
to monitor the surface of the Earth (`official website <http://www.esa.int/Our_Activities/Observing_the_Earth/Copernicus/Sentinel-2>`_).
Sets of images are taken of the surface where each image corresponds to a
specific wavelength. These images can provide useful data for a wide variety of
industries, however, the format they are stored can prove difficult to work with. This being, JPEG 2000,
an image compression format for jpegs that allow for improved quality and compression ratio.

There are few programs that can work with ``jp2``, which can make processing large amounts of them
difficult. Because of GeoPySpark, though, we can leverage the tools available to us in python that
can work with ``jp2`` and use them to ingest sentinel data.


Geting the Data
================

Before we can start this tutorial, we will need to get the sentinel images.
All sentinel data can be found on Amazon's S3, and we will be downloading it straight
from there.

The way the actual data is stored on S3 will not be discussed here, instead, I will just got over
a few details. We will downloading three different ``jp2`` that represent the same area and time
in different wavelength. These being: Aerosol detection (443 nm), Water vapor (945 nm), and Cirrus (1375 nm).
Why these three bands? It's because of the resolution of the image, which determines what bands
are represented best. For this example, we will be working at a 60 m resolution. As for the what is in the
photos, it is the east coast of Corsica taken on January 4th, 2017.

All of the above creates the following base url for this image set, which we will assigne to the ``baseurl``
variable in the terminal:

.. code-block:: none

  baseurl="http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/NM/2017/1/4/0/"


To download the bands, we just have to ``wget`` each one, and then move the resulting
``jp2`` to ``/tmp``.

.. code-block:: none

 wget ${baseurl}B01.jp2
 wget ${baseurl}B09.jp2
 wget ${baseurl}B10.jp2
 mv B01.jp2 B09.jp2 B10.jp2 /tmp


Now that we have our data, we can now begin the ingest process.


The Code
=========

.. code-block:: python

  import numpy as np
  import rasterio

  from geopyspark.geopycontext import GeoPyContext
  from geopyspark.geotrellis.constants import SPATIAL, ZOOM
  from geopyspark.geotrellis.catalog import write
  from geopyspark.geotrellis.rdd import RasterRDD

  geopysc = GeoPyContext(appName="sentinel-ingest", master="local[*]")

  jp2s = ["/tmp/B01.jp2", "/tmp/B09.jp2", "/tmp/B10.jp2"]
  arrs = []

  # Reading the jp2s with rasterio
  for jp2 in jp2s:
      with rasterio.open(jp2) as f:
          arrs.append(f.read(1))

  data = np.array(arrs, dtype=arrs[0].dtype)

  # saving the max and min values of the tile
  with open('/tmp/sentinel_stats.txt', 'w') as f:
      f.writelines([str(data.max()) + "\n", str(data.min())])

  if f.nodata:
      no_data = f.nodata
  else:
      no_data = 0

  bounds = f.bounds
  epsg_code = int(f.crs.to_dict()['init'][5:])

  # Creating the RasterRDD
  tile = {'data': data, 'no_data_value': no_data}

  extent = {'xmin': bounds.left, 'ymin': bounds.bottom, 'xmax': bounds.right, 'ymax': bounds.top}
  projected_extent = {'extent': extent, 'epsg': epsg_code}

  rdd = geopysc.pysc.parallelize([(projected_extent, tile)])
  raster_rdd = RasterRDD.from_numpy_rdd(geopysc, SPATIAL, rdd)

  metadata = raster_rdd.collect_metadata()
  laid_out = raster_rdd.tile_to_layout(metadata)
  reprojected = laid_out.reproject("EPSG:3857", scheme=ZOOM)

  pyramided = reprojected.pyramid(start_zoom=12, end_zoom=1)

  for tiled in pyramided:
      write("file:///tmp/sentinel-catalog", "sentinel-benchmark", tiled)


Running the Code
-----------------

Running the code is simple, and you have two different ways of doing it.

The first is to copy and paste the code into a console like, iPython, and then
running it.

The second is to place this code in a python file and then saving it. To run it
from the file, go to the directory the file is in and run this command

.. code-block:: none

  python3 file.py

Just replace ``file.py`` with whatever name you decided to call the file.

Breaking Down the Code
=======================

Let's now see what's going on through the code by going through each step of the process.
**Note**: This section will only cover the reading in and formatting the data steps. For a guide through
each ingest step, please see :ref:`break_down`.


The Imports
------------

The one note to make here is:

.. code-block:: python

  import rasterio
  import numpy as np

We will need ``rasterio`` to read in the `jp2`` and ``numpy`` to format the data so
that it can be used with GeoPySpark.


Reading in the Jpeg 2000s
--------------------------

.. code-block:: python

  jp2s = ["/tmp/B01.jp2", "/tmp/B09.jp2", "/tmp/B10.jp2"]
  arrs = []

  # Reading the jp2s with rasterio
  for jp2 in jp2s:
      with rasterio.open(jp2) as f:
          arrs.append(f.read(1))

  data = np.array(arrs, dtype=arrs[0].dtype)

As mentioned in the previous section, ``rasterio`` will be used to read in our ``jp2``.
Because each image represents a wavelength, there is a order in which they need to be in
when turned into a multiband tile which is represented by the above list. When read in,
the ``jp2`` will be converted to ``numpy`` arrays.


Saving the Whole Image Stats
-----------------------------

.. code-block:: python

 # saving the max and min values of the tile
 with open('/tmp/sentinel_stats.txt', 'w') as f:
     f.writelines([str(data.max()) + "\n", str(data.min())])

Since the data will need to be converted to ``uint8`` data type in order to be created as a
RGB image. In order to do that, though, we will need to normalize the data so that each point
falls between 0 and 255. This posses a proble, since only a section is read in and rendered at a
time, there is no way of normalizing correctly; as we do not know the entire range of values from the
original image. This is why we must save the ``max`` and ``min`` in a seperate file to read in later.

Formatting the Data
--------------------

.. code-block:: python

  if f.nodata:
      no_data = f.nodata
  else:
      no_data = 0

  bounds = f.bounds
  epsg_code = int(f.crs.to_dict()['init'][5:])

  # Creating the RasterRDD
  tile = {'data': data, 'no_data_value': no_data}

  extent = {'xmin': bounds.left, 'ymin': bounds.bottom, 'xmax': bounds.right, 'ymax': bounds.top}
  projected_extent = {'extent': extent, 'epsg': epsg_code}

  rdd = geopysc.pysc.parallelize([(projected_extent, tile)])
  raster_rdd = RasterRDD.from_numpy_rdd(geopysc, SPATIAL, rdd)

GeoPySpark is a python binding of GeoTrellis, and because of that, requires the data being worked with
to be in a certain format. Please see `ref`:core_concepts` to learn what each of these variables represent.


Ingesting the Data
-------------------

All that remains now is to ingest the data. These steps can be followed at :ref:`break_down`.
