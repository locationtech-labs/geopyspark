What is GeoPySpark?
--------------------

*GeoPySpark* is a python language binding library of the scala library,
`GeoTrellis <https://github.com/locationtech/geotrellis>`_. Like GeoTrellis,
this project is released under the Apache 2 License.

GeoPySpark seeks to utilize GeoTrellis to allow for the reading, writing, and
operating on raster data. Thus, its able to scale to the data and still be able
to perform well.

In addition to raster processing, GeoPySpark allows for rasters to be rendered
into PNGs. One of the goals of this project to be able to process rasters at
web speeds and to perform batch processing of large data sets.

Why GeoPySpark?
----------------

Raster processing in Python has come a long way; however, issues still arise
as the size of the dataset increases. Whether it is performance or ease of use,
these sorts of problems will become more common as larger amounts of data are
made available to the public.

One could turn to GeoTrellis to resolve the aforementioned problems (and one
should try it out!), yet this brings about new challanges. Scala, while a
powerful language, has something of a steep learning curve. This can put off
those who do not have the time and/or interest in learning a new language.

By having the speed and scalability of Scala and the ease of Python,
GeoPySpark is then the remedy to this predicament.

Contact and Support
--------------------

If you need help, have questions, or like to talk to the developers (let us
know what you're working on!) you contact us at:

 * `Gitter <https://gitter.im/geotrellis/geotrellis>`_
 * `Mailing list <https://locationtech.org/mailman/listinfo/geotrellis-user>`_

As you may have noticed from the above links, those are links to the GeoTrellis
gitter channel and mailing list. This is because this project is currently an
offshoot of GeoTrellis, and we will be using their mailing list and gitter
channel as a means of contact. However, we will form our own if there is
a need for it.


.. toctree::
  :maxdepth: 2
  :caption: Home
  :hidden:

  changelog <CHANGELOG>
  contributing

.. toctree::
  :maxdepth: 3
  :caption: User Guides
  :glob:
  :hidden:

  guides/core_concepts

.. toctree::
  :maxdepth: 3
  :caption: Tutorials
  :glob:
  :hidden:

  tutorials/greyscale_ingest_example
  tutorials/greyscale_tile_server_example
  tutorials/sentinel_ingest_example
  tutorials/sentinel_tile_server_example

.. toctree::
  :maxdepth: 4
  :caption: Docs
  :glob:
  :hidden:

  docs/geopyspark
  docs/geopyspark.geotrellis
