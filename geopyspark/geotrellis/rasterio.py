import os
import math
import geopyspark as gps
try:
    import rasterio
except ImportError:
    raise ImportError("rasterio must be installed in order to use the features in the geopyspark.geotrellis.rasterio package")


__all__ = ['read_windows', 'get']

# On driver
_GDAL_DATA = os.environ.get("GDAL_DATA")

def crs_to_proj4(crs):
    """Converts RasterIO CRS to proj4 string using osgeo library.

    Args:
        crs: :class:`rasterio.crs.CRS`

    Returns:
        Proj4 string of CRS.
    """
    try:
        from osgeo import osr
    except ImportError:
        raise ImportError("osgeo must be installed in order to use the crs_to_proj4 function")

    srs = osr.SpatialReference()
    srs.ImportFromWkt(crs.wkt)
    proj4 = srs.ExportToProj4()
    return proj4

def read_windows(uri, xcols=256, ycols=256, bands=None, crs_to_proj4=crs_to_proj4):
    """Given a URI, this method uses rasterio to generate series of windows of the desired dimensions.

    Args:
        uri: The URI where the source data can be found.
        xcols: The desired tile width.
        ycols: The desired tile height.
        bands: The bands from which windows should be produced.  An array of integers.
        crs_to_proj4: A that takes a :class:`rasterio.crs.CRS` and returns a Proj4 string.

    Returns:
        Generator of (:class:`~geopyspark.geotrellis.ProjectedExtent`, :class:`~geopyspark.geotrellis.Tile`)
    """

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

def get(uri):
    """Creates a ``RDD`` of windows from URIs using rasterio.

    Args:
        uri (str or [str]): The path or list of paths to the desired tile(s)/directory(ies).

    Returns:
        RDD
    """
    pysc = gps.get_spark_context()

    if isinstance(uri, str):
        uri = [uri]

    return pysc.parallelize(uri, len(uri)).flatMap(read_windows)
