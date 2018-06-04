import os
import math

import geopyspark as gps
from geopyspark.geotrellis.constants import DEFAULT_MAX_TILE_SIZE

try:
    import rasterio
except ImportError:
    raise ImportError("rasterio must be installed in order to use the features in the geopyspark.geotrellis.rasterio package")


__all__ = ['get']

# On driver
_GDAL_DATA = os.environ.get("GDAL_DATA")

def crs_to_proj4(crs):
    """Converts a ``rasterio.crsCRS`` to a proj4 str using osgeo library.

    Args:
        crs (``rasterio.crs.CRS``): The target ``CRS`` to be converted to a proj4 str.

    Returns:
        Proj4 str of the ``CRS``.
    """

    try:
        from osgeo import osr
    except ImportError:
        raise ImportError("osgeo must be installed in order to use the crs_to_proj4 function")

    srs = osr.SpatialReference()
    srs.ImportFromWkt(crs.wkt)
    proj4 = srs.ExportToProj4()
    return proj4

def _read_windows(uri, xcols, ycols, bands, crs_to_proj4):

    if ("GDAL_DATA" not in os.environ) and (_GDAL_DATA != None):
        os.environ["GDAL_DATA"] = _GDAL_DATA

    with rasterio.open(uri) as dataset:
        bounds = dataset.bounds
        height = dataset.height
        width = dataset.width
        proj4 = crs_to_proj4(dataset.get_crs())
        nodata = dataset.nodata
        tile_cols = (int)(math.ceil(width/xcols)) * xcols
        tile_rows = (int)(math.ceil(height/ycols)) * ycols
        windows = [((x, min(width-1, x + xcols)), (y, min(height-1, y + ycols)))
                   for x in range(0, tile_cols, xcols)
                   for y in range(0, tile_rows, ycols)]

        for window in windows:
            ((row_start, row_stop), (col_start, col_stop)) = window

            left = bounds.left + (bounds.right - bounds.left)*(float(col_start)/width)
            right = bounds.left + (bounds.right - bounds.left)*(float(col_stop)/ width)
            bottom = bounds.top + (bounds.bottom - bounds.top)*(float(row_stop)/height)
            top = bounds.top + (bounds.bottom - bounds.top)*(float(row_start)/height)
            extent = gps.Extent(left, bottom, right, top)
            projected_extent = gps.ProjectedExtent(extent=extent, proj4=proj4)

            data = dataset.read(bands, window=window)
            tile = gps.Tile.from_numpy_array(data, no_data_value=nodata)
            yield (projected_extent, tile)

def get(data_source,
        xcols=DEFAULT_MAX_TILE_SIZE,
        ycols=DEFAULT_MAX_TILE_SIZE,
        bands=None,
        crs_to_proj4=crs_to_proj4):
    """Creates an ``RDD`` of windows represented as the key value pair: ``(ProjectedExtent, Tile)``
    from URIs using rasterio.

    Args:
        data_source (str or [str] or RDD): The source of the data to be windowed.
            Can either be URI or list of URIs which point to where the source data can be found;
            or it can be an ``RDD`` that contains the URIs.
        xcols (int, optional): The desired tile width. If the size is smaller than
            the width of the read in tile, then that tile will be broken into smaller sections
            of the given size. Defaults to :const:`~geopyspark.geotrellis.constants.DEFAULT_MAX_TILE_SIZE`.
        ycols (int, optional): The desired tile height. If the size is smaller than
            the height of the read in tile, then that tile will be broken into smaller sections
            of the given size. Defaults to :const:`~geopyspark.geotrellis.constants.DEFAULT_MAX_TILE_SIZE`.
        bands ([int], opitonal): The bands from which windows should be produced given as a list
            of ``int``\s. Defaults to ``None`` which causes all bands to be read.
        crs_to_proj4 (``rasterio.crs.CRS`` => str, optional) A funtion that takes a :class:`rasterio.crs.CRS`
            and returns a Proj4 string. Default is :func:`geopyspark.geotrellis.rasterio.crs_to_proj4`.

    Returns:
        RDD
    """

    pysc = gps.get_spark_context()

    if isinstance(data_source, (list, str)):
        if isinstance(data_source, str):
            data_source = [data_source]

        return pysc.\
                parallelize(data_source, len(data_source)).\
                flatMap(lambda ds: _read_windows(ds, xcols, ycols, bands, crs_to_proj4))
    else:
        return data_source.flatMap(lambda ds: _read_windows(ds, xcols, ycols, bands, crs_to_proj4))
