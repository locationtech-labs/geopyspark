import io
from PIL import Image


class PngRDD(object):
    def __init__(self, geopysc, rdd_type, tiledrdd, rampname):
        self.geopysc = geopysc
        self.rdd_type = rdd_type
        self.layer_metadata = tiledrdd.layer_metadata
        self.srdd = self.geopysc._jvm.geopyspark.geotrellis.PngRDD.asSingleband(tiledrdd.srdd, rampname)

    def lookup(self, col, row):
        """Return the value(s) in the image of a particular SpatialKey (given by col and row)

        Args:
            col (int): The SpatialKey column
            row (int): The SpatialKey row

        Returns: An array of numpy arrays (the tiles)
        """
        bounds = self.layer_metadata['bounds']
        min_col = bounds['minKey']['col']
        min_row = bounds['minKey']['row']
        max_col = bounds['maxKey']['col']
        max_row = bounds['maxKey']['row']

        if col < min_col or col > max_col:
            raise IndexError("column out of bounds")
        if row < min_row or row > max_row:
            raise IndexError("row out of bounds")

        result = self.srdd.lookup(col, row)

        return [Image.open(io.BytesIO(bytes)) for bytes in result]
