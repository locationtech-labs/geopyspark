#!/usr/bin/env bash

USER=$1
GROUP=$2

export CPPFLAGS="-I$HOME/local/gdal/include"
export CFLAGS="-I$HOME/local/gdal/include"
export LDFLAGS="-I$HOME/local/gdal/lib"

# untar gdal and friends
mkdir -p $HOME/local/gdal
cd $HOME/local/gdal
tar axvf /archives/gdal-and-friends.tar.gz

# install geopsypark and friends
cd $HOME/
chown -R root:root $HOME/.cache/pip
pip3 install --user -r /scripts/requirements.txt
pip3 install --user /archives/geopyspark-0.1.0-py3-none-any.whl
pip3 install --user rasterio==1.0a7

# archive libraries
cd $HOME/.local/lib/python3.4/site-packages
tar acvf /archives/geopyspark-and-friends.tar.gz .

chown -R $USER:$GROUP /archives $HOME/.cache/pip
