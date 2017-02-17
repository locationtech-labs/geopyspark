# GeoPySpark

`geopyspark` provides python bindings for working with geospatial data on [PySpark](http://spark.apache.org/docs/latest/api/python/pyspark.html)
It will provide interfaces into GeoTrellis and GeoMesa LocationTech frameworks.
It is currently under development, and is pre-alpha quality.

## Make targets

 - `isntall` - install `geopyspark` python package locally
 - `wheel` - build python `geopyspark` wheel for distribution
 - `pyspark` - start pyspark shell with project jars
 - `docker-build` - build docker image for Jupyter with `geopyspark`
 - `docker-run` - run docker image for Jupyter with `geopyspark`
 - `docker-exec` - start a shell in current docker image
