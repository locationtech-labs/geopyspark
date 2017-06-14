from geopyspark.geopyspark_utils import check_environment
check_environment()

from geopyspark.geotrellis.constants import RESAMPLE_METHODS, NEARESTNEIGHBOR, ZOOM, COLOR_RAMPS
from geopyspark.geotrellis.rdd import CachableRDD
from geopyspark.geotrellis.render import PngRDD
from pyspark.storagelevel import StorageLevel

# Keep it in non rendered form so we can do map algebra operations to it
class Pyramid(CachableRDD):
    def __init__(self, levels):
        """List of TiledRasterRDDs into a coherent pyramid tile pyramid.

        Args:
            pyramid (list): A list of TiledRasterRDD.

        """

        level0 = levels[0]
        self.levels = levels
        self.pyramid = dict([(l.zoom_level, l) for l in levels])
        self.geopysc = level0.geopysc
        self.rdd_type = level0.rdd_type
        self.max_zoom = max(self.pyramid.keys())
        self.is_cached = False
        self.histogram = None

    def wrapped_rdds(self):
        print(self.levels.values())
        return self.levels.values()

    def get_histogram(self):
        if not self.histogram:
            self.histogram = self.pyramid[self.max_zoom].get_histogram()
        return self.histogram

    def to_png_pyramid(self, color_ramp=None, color_map=None, num_breaks=10):
        if not color_map and color_ramp:
            hist = self.get_histogram()
            breaks = hist.quantileBreaks(num_breaks)
            color_map = self.geopysc._jvm.geopyspark.geotrellis.Coloring.makeColorMap(breaks, color_ramp)

        return PngRDD(self.levels, color_map)

    def __add__(self, value):
        if isinstance(value, Pyramid):
            def common_entries(*dcts):
                for i in set(dcts[0]).intersection(*dcts[1:]):
                    yield (i,) + tuple(d[i] for d in dcts)
            new_levels = [ (l+r) for (k, l, r) in common_entries(self.pyramid, value.pyramid)]
            return Pyramid(new_levels)
        else:
            return Pyramid([l.__add__(value) for l in self.levels])

    def __radd__(self, value):
        return Pyramid([l.__radd__(value) for l in self.levels])

    def __sub__(self, value):
        return Pyramid([l.__sub__(value) for l in self.levels])

    def __rsub__(self, value):
        return Pyramid([l.__rsub__(value) for l in self.levels])

    def __mul__(self, value):
        return Pyramid([l.__mul__(value) for l in self.levels])

    def __rmul__(self, value):
        return Pyramid([l.__rmul__(value) for l in self.levels])

    def __truediv__(self, value):
        return Pyramid([l.__truediv__(value) for l in self.levels])

    def __rtruediv__(self, value):
        return Pyramid([l.__rtruediv__(value) for l in self.levels])
