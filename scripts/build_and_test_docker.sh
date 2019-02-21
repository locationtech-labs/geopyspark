#!/bin/bash

docker run -it --net=host \
  -v $TRAVIS_BUILD_DIR:/geopyspark \
  -v $HOME/archives:/root/archives \
  -v $HOME/.coursier:/root/.coursier \
  -v $HOME/.ivy2:/root/.ivy2 \
  -v $HOME/.sbt:/root/.sbt \
  -e TRAVIS_PYTHON_VERSION=$TRAVIS_PYTHON_VERSION \
  -e TRAVIS_COMMIT=$TRAVIS_COMMIT \
  -e COURSIER_PROGRESS=false \
  -e COURSIER_NO_TERM=true \
  -e PYSPARK_PYTHON=/usr/bin/python3.5 \
  -e PYSPARK_DRIVER_PYTHON=/usr/bin/python3.5 \
  daunnc/openjdk-gdal:2.4.0 bash -c "cd /geopyspark && ./scripts/run_tests.sh"
