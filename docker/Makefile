SRC := archives/gdal-2.1.3.tar.gz archives/geos-3.6.1.tar.bz2 archvies/lcms2-2.8.tar.gz archives/libpng-1.6.28.tar.gz archives/proj-4.9.3.tar.gz archives/v2.1.2.tar.gz archives/zlib-1.2.11.tar.gz
MOOP := archives/geopyspark-0.1.0-py3-none-any.whl

all: archives/gdal-and-friends.tar.gz

# archives/geopyspark-and-friends.tar.gz

archives/gdal-and-friends.tar.gz: $(SRC)
	docker run -it --rm -v $(pwd)/archives:/archives:rw -v $(pwd)/scripts:/scripts:ro quay.io/geodocker/jupyter-geopyspark:stage0 --entry-point /scripts/build-blobs.sh $(id -u) $(id -g)
