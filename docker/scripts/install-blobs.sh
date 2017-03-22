#!/usr/bin/env bash

# untar gdal and friends
mkdir -p $HOME/local/gdal
cd $HOME/local/gdal
tar axvf /blobs/gdal-and-friends.tar.gz

# untar geopyspark and friends
mkdir -p $HOME/.local/lib/python3.4/site-packages
cd $HOME/.local/lib/python3.4/site-packages
tar axvf /blobs/geopyspark-and-friends.tar.gz
