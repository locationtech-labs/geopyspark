#!/bin/bash

docker run -it --net=host \
  -v $HOME/geopyspark:/geopyspark \
  -e TRAVIS_PYTHON_VERSION=$TRAVIS_PYTHON_VERSION \
  -e TRAVIS_COMMIT=$TRAVIS_COMMIT \
  daunnc/openjdk-gdal:2.4.0 /bin/bash "cd $HOME/geopyspark; test_script.sh"
