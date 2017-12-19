Reading and Rasterizing Open Street Map Data
---------------------------------------------

This tutorial shows how to read in Open Street Map (OSM) data, and then
rasterize it using GeoPySpark.

**Note**: This guide is aimed at users who are already familiar with GeoPySpark.


Getting the Data
=================

To start, let's first grab an `orc file <https://orc.apache.org/>`__,
which a special file type that is optimized for Hadoop operations.
The following command will use ``curl`` to download the file from
``S3`` and move it to the ``/tmp`` directory.

.. code::

  curl -o /tmp/boyertown.orc https://s3.amazonaws.com/geopyspark-test/example-files/boyertown.orc

**A side note**: Files can be retrieved directly from S3. However, this could
not be done in this instance due to permission requirements needed to access
the file.


Reading in the Data
====================

Now that we have our data, we can now read it in and begin to work with.

.. code:: python3

   import geopyspark as gps
   from pyspark import SparkContext

   conf = gps.geopyspark_conf(appName="osm-rasterize-example", master="local[*]")
   pysc = SparkContext(conf=conf)

   features = gps.osm_reader.from_orc("/tmp/boyertown.orc")

The above code sets up a ``SparkContext`` and then reads in the
``boyertown.orc`` file as ``features``, which is an instance
of ``FeaturesCollection``.

When OSM data is read into GeoPySpark, each OSM Element is turned into
single or multiple different geometries. With each of these geometries
retaining the metadata from the derived OSM Element. These geometry metadata
pairs are referred to as a ``Feature``. These features are grouped together by
the type of geometry they contain. When accessing features from a
``FeaturesCollection``, it is done by geometry.

There are four different types of geometries in the ``FeaturesCollection``:

  - ``Point``
  - ``Line``
  - ``Polygon``
  - ``MultiPolygon``


Selecting the Features We Want
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For this example, we're interested in rasterizing the ``Line``\s and
``Polygon``\s from the OSM data, so we will select those ``Feature``\s
from the ``FeaturesCollection`` that contain them. The following code will
create a Python ``RDD`` of ``Feature``\s that contains all ``Line`` geometries
(``lines``), and a Python ``RDD`` that contains all ``Polygon``
geometries (``polygons``).

.. code:: python3

   lines = features.get_line_features_rdd()
   polygons = features.get_polygon_features_rdd()


Looking at the Tags of the Features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we rasterize the ``Polygon`` ``Feature``\s, we'd like for schools to have
a different value than all of the other ``Polygon``\s.  However, we are unsure
if any schools were contained within the original data, and we'd like to see
if any are. One method we could use to determine if there are schools is to
look at the tags of the ``Polygon`` ``Feature``\s.  The following code will show
all of the unique tags for all of the ``Polygon``\s in the collection.

.. code:: python3

   features.get_polygon_tags()

Which has the following output:

.. code::

   {'NHD:ComID': '25964412',
    'NHD:Elevation': '0.00000000000',
    'NHD:FCode': '39004',
    'NHD:FDate': '2001/08/16',
    'NHD:FTYPE': 'LakePond',
    'NHD:Permanent_': '25964412',
    'NHD:ReachCode': '02040203004486',
    'NHD:Resolution': 'High',
    'addr:city': 'Gilbertsville',
    'addr:housenumber': '1100',
    'addr:postcode': '19525',
    'addr:state': 'PA',
    'addr:street': 'E Philadelphia Avenue',
    'amenity': 'school',
    'area': 'yes',
    'building': 'yes',
    'leisure': 'pitch',
    'name': 'Boyertown Area Junior High School-West Center',
    'natural': 'water',
    'railway': 'platform',
    'smoking': 'outside',
    'source': 'Yahoo',
    'sport': 'baseball',
    'tourism': 'museum',
    'wikidata': 'Q8069423',
    'wikipedia': "en:Zern's Farmer's Market"}


So it appears that there are schools in this dataset, and that we can continue
on.


Assigning Values to Geometries
===============================

Now that we have our ``Feature``\s, it's time to assign them values. The
reason we need to do so is because when a vector becomes a raster, its cells
need to have some kind of value. When rasterizing ``Feature``\s, each geometry
contained within it will be given a single value, and all cells that intersect
that shape will have that value. In addition to value of the actual cells,
there's another property that we will want to set for each
``Feature``, ``Z-Index``.

The ``Z-Index`` of a ``Feature`` determines what value a cell will be if more
than one geometry intersects it. With a higher ``Z-Index`` taking priority over
a lower one. This is important as there may be cases where multiple geometries
are present at a single cell, but that cell can only contain one value.

For this example, we are going to want all ``Polygon``\s to have a higher
``Z-Index`` than the ``Line``\s. In addition, since we're interested in
schools, ``Polygon``\s that are labeled as schools will have a greater
``Z-Index`` than other ``Polygon``\s.

.. code:: python3

   mapped_lines = lines.map(lambda feature: gps.Feature(feautre.geometry, gps.CellValue(value=1, zindex=1)))

   def assign_polygon_feature(feature):
       tags = feature.properties.tags.values()

       if 'school' in tags.values():
           return gps.Feature(feature.geometry, gps.CellValue(value=3, zindex=3))
       else:
           return gps.Feature(feature.geometry, gps.CellValue(value=2, zindex=2))

   mapped_polygons = polygons.map(assign_polygon_feature)

We create the ``mapped_lines`` variable that contains an ``RDD`` of ``Feature``\s,
where each ``Feature`` has a ``CellValue`` with a ``value`` and ``zindex`` of 1.
The ``assign_polygon_feature`` function is then created which will test to see if a
``Polygon`` is a school or not. If it is, then the resulting ``Feature`` will
have a ``CellValue`` with a ``value`` and ``zindex`` of 3. Otherwise, those
two values will be 2.


Rasterizing the Features
=========================

Now that the ``Feature``\s have been given ``CellValue``\s, it is now time to
rasterize them.

.. code:: python3

   unioned_features = pysc.union((mapped_lines, mapped_polygons))

   rasterized_layer = gps.rasterize_features(features=unioned_features, crs=4326, zoom=12)

The ``rasterize_features`` function requires a single ``RDD`` of ``Feature``\s.
Therefore, we union together ``mapped_lines`` and ``mapped_polygons`` which
gives us ``unioned_features``. Along with passing in our ``RDD``, we must also
set the ``crs`` and ``zoom`` of the resulting Layer. In this case, the ``crs``
is in ``LatLng``, so we set it to be ``4326``. ``zoom`` varies between use cases,
so it was just chosen arbitrarily for this example. The resulting
``rasterized_layer`` is a ``TiledRasterLayer`` that we can now analyze and/or
ingest.
