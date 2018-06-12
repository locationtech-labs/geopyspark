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
pairs are referred to as a ``Feature``. These features and their respective
geometries are grouped together by the type OSM object the geometry represented.
When accessing features from a ``FeaturesCollection``, it is done by the
represented OSM type.

There are three different types of OSM features in the ``FeaturesCollection``:

  - ``Node``\s
  - ``Way``\s
  - ``Relation``\s


Selecting the Features We Want
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For this example, we're interested in rasterizing the ``Ways``\s
from the OSM data, so we will select those ``Feature``\s
from the ``FeaturesCollection`` that contain them. The following code will
create a Python ``RDD`` of ``Feature``\s that contains all ``Way`` geometries
and their metadata.


.. code:: python3

   ways = features.get_way_features_rdd()


Looking at the Tags of the Features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When we rasterize the ``Feature``\s, we'd like for schools to have
a different value than all of the other ``Feature``\s.  However, we are unsure
if any schools were contained within the original data, and we'd like to see
if any are. One method we could use to determine if there are schools is to
look at the tags of the ``Way`` ``Feature``\s.  The following code will show
all of the unique tags for all of the ``Way``\s in the collection.

.. code:: python3

   features.get_way_tags()

Which has the following output:

.. code::

  {'NHD:ComID': '25964938',
   'NHD:Elevation': '0.00000000000',
   'NHD:FCode': '46006',
   'NHD:FDate': '2001/08/16',
   'NHD:FTYPE': 'LakePond',
   'NHD:FType': 'StreamRiver',
   'NHD:Permanent_': '25964312',
   'NHD:RESOLUTION': 'High',
   'NHD:ReachCode': '2040203001298',
   'NHD:Resolution': 'High',
   'NHD:way_id': '25964938',
   'NHS': 'yes',
   'addr:city': 'Boyertown',
   'addr:housenumber': '709',
   'addr:postcode': '19512',
   'addr:street': 'East Philadelphia Avenue',
   'amenity': 'school',
   'area': 'yes',
   'attribution': 'NHD',
   'bridge': 'yes',
   'building': 'house',
   'cuisine': 'pizza',
   'delivery': 'no',
   'destination': 'Boyertown;Gilbertsville',
   'destination:ref': 'PA 73',
   'dispensing': 'yes',
   'drive_through': 'yes',
   'electrified': 'no',
   'expressway': 'yes',
   'gauge': '1435',
   'gnis:feature_id': '2487880',
   'healthcare': 'optometrist',
   'hgv': 'yes',
   'highway': 'service',
   'landuse': 'cemetery',
   'lanes': '2',
   'layer': '1',
   'leisure': 'stadium',
   'name': 'East 2nd Street',
   'name_1': 'Big Road',
   'name_2': 'Layfield Rd',
   'natural': 'water',
   'office': 'accountant',
   'old_railway_operator': 'Reading',
   'old_ref:legislative': '197',
   'oneway': 'yes',
   'operator': 'Santander',
   'outdoor_seating': 'no',
   'railway': 'platform',
   'ref': 'PA 73',
   'service': 'alley',
   'shop': 'beauty',
   'source': 'Yahoo',
   'source:hgv': 'PennDOT ftp://ftp.dot.state.pa.us/public/Bureaus/BOMO/MC/Publication411.pdf',
   'sport': 'american_football',
   'takeaway': 'yes',
   'tiger:cfcc': 'A41',
   'tiger:county': 'Berks, PA',
   'tiger:name_base': '2nd',
   'tiger:name_base_1': 'State Route 73',
   'tiger:name_base_2': 'State Route 73',
   'tiger:name_base_3': 'Layfield',
   'tiger:name_direction_prefix': 'E',
   'tiger:name_type': 'St',
   'tiger:name_type_1': 'Rd',
   'tiger:name_type_3': 'Rd',
   'tiger:reviewed': 'no',
   'tiger:source': 'tiger_import_dch_v0.6_20070829',
   'tiger:tlid': '80457400:80457404',
   'tiger:upload_uuid': 'bulk_upload.pl-6517a127-4250-40d5-88da-8acebe246150',
   'tiger:zip_left': '19512',
   'tiger:zip_left_1': '19512',
   'tiger:zip_right': '19512',
   'usage': 'freight',
   'waterway': 'stream'}


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

For this example, we are going to want all buildings to have a higher
``Z-Index`` than roadways and other lines. In addition, since we're interested in
schools, ``Geometry``\s that are labeled as schools will have a greater
``Z-Index`` than others.

.. code:: python3

   def assign_way_feature(feature):
       tags = feature.properties.tags.values()

       if 'school' in tags.values():
           return gps.Feature(feature.geometry, gps.CellValue(value=3, zindex=3))
       elif ('building', 'true') in tags.items():
           return gps.Feature(feature.geometry, gps.CellValue(value=2, zindex=2))
       else:
           return gps.Feature(feautre.geometry, gps.CellValue(value=1, zindex=1))

   mapped_ways = ways.map(assign_way_feature)


The ``assign_way_feature`` function test to see if a ``Geometry`` is a school or not.
If it is, then the resulting ``Feature`` will have a ``CellValue`` with a ``value`` and
``zindex`` of 3. In the case where the ``Geometry`` is a building, but not a school,
then the two values will both be 2. Otherwise, those two values will be 1.


Rasterizing the Features
=========================

Now that the ``Feature``\s have been given ``CellValue``\s, it is now time to
rasterize them.

.. code:: python3

   rasterized_layer = gps.rasterize_features(features=mapped_ways, crs=4326, zoom=12)

Along with passing in our ``RDD``, we must also set the ``crs`` and ``zoom`` of the
resulting Layer. In this case, the ``crs`` is in ``LatLng``, so we set it to be
``4326``. ``zoom`` varies between use cases, so it was just chosen arbitrarily for
this example. The resulting ``rasterized_layer`` is a ``TiledRasterLayer`` that we
can now analyze and/or ingest.
