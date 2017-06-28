from geopyspark.geopyspark_utils import ensure_pyspark
ensure_pyspark()

from geopyspark.geotrellis.constants import ResampleMethod, LayoutScheme
from .layer import CachableLayer
from pyspark.storagelevel import StorageLevel
import geopyspark.geotrellis.color as color
from geopyspark.geotrellis import deprecated

@deprecated
def get_breaks_from_colors(colors):
    """Deprecated in favor of geopyspark.geotrellis.color.get_breaks_from_colors
    """
    return color.get_breaks_from_colors(colors)

@deprecated
def get_breaks_from_matplot(ramp_name, num_colors):
    """Deprecated in favor of geopyspark.geotrellis.color.get_breaks_from_matplot
    """
    return color.get_breaks_from_matplot(ramp_name, num_colors)

@deprecated
def get_breaks(pysc, ramp_name, num_colors=None):
    """Deprecated in favor of geopyspark.geotrellis.color.get_breaks
    """
    return color.get_breaks(pysc, ramp_name, num_colors=None)

@deprecated
def get_hex(pysc, ramp_name, num_colors=None):
    """Deprecated in favor of geopyspark.geotrellis.color.get_hex
    """
    return color.get_hex(pysc, ramp_name, num_colors=None)
