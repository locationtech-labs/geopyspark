
Reading in Sentinel-2 Images
============================

Sentinel-2 is an observation mission developed by the European Space
Agency to monitor the surface of the Earth `official
website <http://www.esa.int/Our_Activities/Observing_the_Earth/Copernicus/Sentinel-2>`__.
Sets of images are taken of the surface where each image corresponds to
a specific wavelength. These images can provide useful data for a wide
variety of industries, however, the format they are stored in can prove
difficult to work with. This being ``JPEG 2000`` (file extension
``.jp2``), an image compression format for JPEGs that allows for
improved quality and compression ratio.

Why Use GeoPySpark
------------------

There are few libraries and/or applications that can work with
``jp2``\ s and big data, which can make processing large amounts of
sentinel data difficult. However, by using GeoPySpark in conjunction
with the tools available in Python, we are able to read in and work with
large sets of sentinel imagery.

Getting the Data
----------------

Before we can start this tutorial, we will need to get the sentinel
images. All sentinel data can be found on Amazon's S3 service, and we
will be downloading it straight from there.

We will download three different ``jp2``\ s that represent the same area
and time in different wavelengths: Aerosol detection (443 nm), Water
vapor (945 nm), and Cirrus (1375 nm). These bands are chosen because
they are all in the same 60m resolution. The tiles we will be working
with cover the eastern coast of Corsica taken on January 4th, 2017.

For more information on the way the data is stored on S3, please see
this
`link <http://sentinel-pds.s3-website.eu-central-1.amazonaws.com/>`__.

.. code:: ipython3

    !curl -o /tmp/B01.jp2 http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/NM/2017/1/4/0/B01.jp2
    !curl -o /tmp/B09.jp2 http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/NM/2017/1/4/0/B09.jp2
    !curl -o /tmp/B10.jp2 http://sentinel-s2-l1c.s3.amazonaws.com/tiles/32/T/NM/2017/1/4/0/B10.jp2

The Code
--------

Now that we have the files, we can begin to read them into GeoPySpark.

.. code:: ipython3

    import rasterio
    import geopyspark as gps
    import numpy as np
    
    from pyspark import SparkContext

.. code:: ipython3

    conf = gps.geopyspark_conf(master="local[*]", appName="sentinel-ingest-example")
    pysc = SparkContext(conf=conf)

Reading in the JPEG 2000's
--------------------------

``rasterio``, being backed by GDAL, allows us to read in the ``jp2``\ s.
Once they are read in, we will then combine the three seperate numpy
arrays into one. This combined array represents a single, multiband
raster.

.. code:: ipython3

    jp2s = ["/tmp/B01.jp2", "/tmp/B09.jp2", "/tmp/B10.jp2"]
    arrs = []
    
    for jp2 in jp2s:
        with rasterio.open(jp2) as f:
            arrs.append(f.read(1))
    
    data = np.array(arrs, dtype=arrs[0].dtype)
    data

Creating the RDD
----------------

With our raster data in hand, we can how begin the creation of a Python
``RDD``. Please see the `core concepts <core-concepts.ipynb>`__ guide
for more information on what the following instances represent.

.. code:: ipython3

    # Create an Extent instance from rasterio's bounds
    extent = gps.Extent(*f.bounds)
    
    # The EPSG code can also be obtained from the information read in via rasterio
    projected_extent = gps.ProjectedExtent(extent=extent, epsg=int(f.crs.to_dict()['init'][5:]))
    projected_extent

You may have noticed in the above code that we did something weird to
get the ``CRS`` from the rasterio file. This had to be done because the
way rasterio formats the projection of the read in rasters is not
compatible with how GeoPySpark expects the ``CRS`` to be in. Thus, we
had to do a bit of extra work to get it into the correct state

.. code:: ipython3

    # Projection information from the rasterio file
    f.crs.to_dict()

.. code:: ipython3

    # The projection information formatted to work with GeoPySpark
    int(f.crs.to_dict()['init'][5:])

.. code:: ipython3

    # We can create a Tile instance from our multiband, raster array and the nodata value from rasterio
    tile = gps.Tile.from_numpy_array(numpy_array=data, no_data_value=f.nodata)
    tile

.. code:: ipython3

    # Now that we have our ProjectedExtent and Tile, we can create our RDD from them
    rdd = pysc.parallelize([(projected_extent, tile)])
    rdd

Creating the Layer
------------------

From the ``RDD``, we can now create a ``RasterLayer`` using the
``from_numpy_rdd`` method.

.. code:: ipython3

    # While there is a time component to the data, this was ignored for this tutorial and instead the focus is just
    # on the spatial information. Thus, we have a LayerType of SPATIAL.
    raster_layer = gps.RasterLayer.from_numpy_rdd(layer_type=gps.LayerType.SPATIAL, numpy_rdd=rdd)
    raster_layer

Where to Go From Here
=====================

By creating a ``RasterLayer``, we can now work with and analyze the data
within it. If you wish to know more about these operations, please see
the following guides: `Layers Guide <layers.ipynb>`__,
[map-algebra-guide], [visulation-guide], and the [catalog-guide].
