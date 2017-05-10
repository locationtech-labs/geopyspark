from geopyspark.geotrellis.constants import NEARESTNEIGHBOR, ZOOM

class PngRDD(object):
    def __init__(self, pyramid, rampname):
        level0 = pyramid[0]
        self.geopysc = level0.geopysc
        self.rdd_type = level0.rdd_type
        self.layer_metadata = list(map(lambda lev: lev.layer_metadata, pyramid))
        self.max_zoom = level0.zoom_level
        self.pngpyramid = list(map(lambda layer: self.geopysc._jvm.geopyspark.geotrellis.PngRDD.asSingleband(layer.srdd, rampname), pyramid))

    @classmethod
    def makePyramid(cls, tiledrdd, rampname, start_zoom=None, end_zoom=0, resamplemethod=NEARESTNEIGHBOR):
        reprojected = tiledrdd.reproject("EPSG:3857", scheme=ZOOM)

        if not start_zoom:
            if reprojected.zoom_level:
                start_zoom = reprojected.zoom_level
            else:
                raise AttributeError("No initial zoom level is available; Please provide a value for start_zoom")

        pyramid = reprojected.pyramid(start_zoom, end_zoom, resamplemethod)

        return cls(pyramid, rampname)

    def lookup(self, col, row, zoom=None):
        """Return the value(s) in the image of a particular SpatialKey (given by col and row)

        Args:
            col (int): The SpatialKey column
            row (int): The SpatialKey row

        Returns: An array of numpy arrays (the tiles)
        """
        if not zoom:
            idx = 0
        else:
            idx = self.max_zoom - zoom

        pngrdd = self.pngpyramid[idx]
        metadata = self.layer_metadata[idx]

        bounds = metadata['bounds']
        min_col = bounds['minKey']['col']
        min_row = bounds['minKey']['row']
        max_col = bounds['maxKey']['col']
        max_row = bounds['maxKey']['row']

        if col < min_col or col > max_col:
            raise IndexError("column out of bounds")
        if row < min_row or row > max_row:
            raise IndexError("row out of bounds")

        result = pngrdd.lookup(col, row)

        return [bytes for bytes in result]
