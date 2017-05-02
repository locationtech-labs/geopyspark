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
pip3 install --user appdirs==1.4.3
pip3 install --user avro-python3==1.8.1
pip3 install --user numpy==1.12.1
pip3 install --user pyparsing==2.2.0
pip3 install --user six==1.10.0
pip3 install --user packaging==16.8
pip3 install --user Shapely==1.6b4
pip3 install --user rasterio==1.0a7
pip3 install --user /archives/geopyspark-0.1.0-py3-none-any.whl

# install geonotebook dependencies
PATH=$PATH:$HOME/local/gdal/bin pip3 install --user GDAL==2.1.3
pip3 install --user requests==2.11.1
pip3 install --user promise==0.4.2
pip3 install --user fiona==1.7.1
pip3 install --user matplotlib==2.0.0
CFLAGS='-DPI=M_PI -DHALFPI=M_PI_2 -DFORTPI=M_PI_4 -DTWOPI=(2*M_PI) -I$HOME/local/gdal/include' pip3 install --user pyproj==1.9.5.1
pip3 install pandas==0.19.2
pip3 install lxml==3.7.3

# archive libraries
cd $HOME/.local/lib/python3.4/site-packages
tar acvf /archives/geopyspark-and-friends.tar.gz .

chown -R $USER:$GROUP /archives $HOME/.cache/pip
