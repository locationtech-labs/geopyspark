#!/usr/bin/env bash

USERID=$1
GROUPID=$2

export CPPFLAGS="-I$HOME/local/gdal/include"
export CFLAGS="-I$HOME/local/gdal/include"
export LDFLAGS="-I$HOME/local/gdal/lib"

mkdir -p $HOME/local/src
cd $HOME/local/src

# untar source
for archive in zlib-1.2.11.tar.gz libpng-1.6.28.tar.gz geos-3.6.1.tar.bz2 proj-4.9.3.tar.gz lcms2-2.8.tar.gz v2.1.2.tar.gz gdal-2.1.3.tar.gz
do
    tar axvf /archives/$archive
done

# build zlib
cd $HOME/local/src/zlib-1.2.11
./configure --prefix=$HOME/local/gdal && make -j && make install

# build libpng
cd $HOME/local/src/libpng-1.6.28
./configure --prefix=$HOME/local/gdal && make -j && make install

# build geos
cd $HOME/local/src/geos-3.6.1
./configure --prefix=$HOME/local/gdal && make -j && make install

# build proj4
cd $HOME/local/src/proj-4.9.3
./configure --prefix=$HOME/local/gdal && make -j && make install

# build lcms2
cd $HOME/local/src/lcms2-2.8
./configure --prefix=$HOME/local/gdal && make -j && make install

# build openjpeg
cd $HOME/local/src/openjpeg-2.1.2
mkdir -p build
cd build
cmake -DCMAKE_C_FLAGS="-I$HOME/local/gdal/include -L$HOME/local/gdal/lib" -DCMAKE_INSTALL_PREFIX="$HOME/local/gdal" ..
make -j
make install

# build gdal
cd $HOME/local/src/gdal-2.1.3
./configure --prefix=$HOME/local/gdal && (make -j33 || make) && make install

# archive binaries
cd $HOME/local/gdal
tar acvf /archives/gdal-and-friends.tar.gz .

chown -R $USERID:$GROUPID /archives
