"""This module contains functions needed to create color maps used in coloring tiles,
PNGs, and GeoTiffs.
"""
import struct
from geopyspark import get_spark_context
from geopyspark.geotrellis.histogram import Histogram
from geopyspark.geopyspark_utils import ensure_pyspark
ensure_pyspark()

from geopyspark.geotrellis.constants import ClassificationStrategy


__all__ = ["get_colors_from_colors", "get_colors_from_matplotlib", "ColorMap"]


def get_colors_from_colors(colors):
    """Returns a list of integer colors from a list of Color objects from the
    colortools package.

    Args:
        colors ([colortools.Color]): A list of color stops using colortools.Color

    Returns:
        [int]
    """

    return [struct.unpack(">L", bytes(c.rgba))[0] for c in colors]

def get_colors_from_matplotlib(ramp_name, num_colors=1<<8):
    """Returns a list of color breaks from the color ramps defined by Matplotlib.

    Args:
        ramp_name (str): The name of a matplotlib color ramp. See the matplotlib documentation for
            a list of names and details on each color ramp.
        num_colors (int, optional): The number of color breaks to derive from the named map.

    Returns:
        [int]
    """

    try:
        import colortools
        import matplotlib.cm as mpc
    except:
       raise Exception('matplotlib>=2.0.0 and colortools>=0.1.2 required')

    ramp = mpc.get_cmap(ramp_name)
    return  [struct.unpack('>L', bytes(map(lambda x: int(x*255), ramp(x / (num_colors - 1)))))[0] for x in range(0, num_colors)]


"""A dict giving the color mapping from NLCD values to colors."""
nlcd_color_map = {
    0  : 0x00000000,
    11 : 0x526095FF,     # Open Water
    12 : 0xFFFFFFFF,     # Perennial Ice/Snow
    21 : 0xD28170FF,     # Low Intensity Residential
    22 : 0xEE0006FF,     # High Intensity Residential
    23 : 0x990009FF,     # Commercial/Industrial/Transportation
    31 : 0xBFB8B1FF,     # Bare Rock/Sand/Clay
    32 : 0x969798FF,     # Quarries/Strip Mines/Gravel Pits
    33 : 0x382959FF,     # Transitional
    41 : 0x579D57FF,     # Deciduous Forest
    42 : 0x2A6B3DFF,     # Evergreen Forest
    43 : 0xA6BF7BFF,     # Mixed Forest
    51 : 0xBAA65CFF,     # Shrubland
    61 : 0x45511FFF,     # Orchards/Vineyards/Other
    71 : 0xD0CFAAFF,     # Grasslands/Herbaceous
    81 : 0xCCC82FFF,     # Pasture/Hay
    82 : 0x9D5D1DFF,     # Row Crops
    83 : 0xCD9747FF,     # Small Grains
    84 : 0xA7AB9FFF,     # Fallow
    85 : 0xE68A2AFF,     # Urban/Recreational Grasses
    91 : 0xB6D8F5FF,     # Woody Wetlands
    92 : 0xB6D8F5FF}    # Emergent Herbaceous Wetlands


class ColorMap(object):
    """A class that wraps a GeoTrellis ColorMap class.

    Args:
        cmap (py4j.java_gateway.JavaObject): The ``JavaObject`` that represents the GeoTrellis ColorMap.

    Attributes:
        cmap (py4j.java_gateway.JavaObject): The ``JavaObject`` that represents the GeoTrellis ColorMap.
    """

    def __init__(self, cmap):
        self.cmap = cmap

    @classmethod
    def build(cls, breaks, colors=None,
              no_data_color=0x00000000, fallback=0x00000000,
              classification_strategy=ClassificationStrategy.LESS_THAN_OR_EQUAL_TO):
        """Given breaks and colors, build a ``ColorMap`` object.

        Args:
            breaks (dict or list or :class:`~geopyspark.geotrellis.Histogram`): If a ``dict`` then a
                mapping from tile values to colors, the latter represented as integers
                e.g., 0xff000080 is red at half opacity. If a ``list`` then tile values that
                specify breaks in the color mapping. If a ``Histogram`` then a histogram from which
                breaks can be derived.
            colors (str or list, optional):  If a ``str`` then the name of a matplotlib color ramp.
                If a ``list`` then either a list of colortools ``Color`` objects or a list
                of integers containing packed RGBA values. If ``None``, then the ``ColorMap`` will
                be created from the ``breaks`` given.
            no_data_color(int, optional): A color to replace NODATA values with
            fallback (int, optional): A color to replace cells that have no
                value in the mapping
            classification_strategy (str or :class:`~geopyspark.geotrellis.constants.ClassificationStrategy`, optional):
                A string giving the strategy for converting tile values to colors. e.g., if
                ``ClassificationStrategy.LESS_THAN_OR_EQUAL_TO`` is specified, and the break map is
                {3: 0xff0000ff, 4: 0x00ff00ff}, then values up to 3 map to red, values from above 3
                and up to and including 4 become green, and values over 4 become the fallback color.

        Returns:
            :class:`~geopyspark.geotrellis.color.ColorMap`
        """

        pysc = get_spark_context()

        if isinstance(breaks, dict):
            return ColorMap.from_break_map(breaks, no_data_color, fallback, classification_strategy)

        if isinstance(colors, str):
            color_list = get_colors_from_matplotlib(colors)
        elif isinstance(colors, list):
            if all(isinstance(c, int) for c in colors):
                color_list = colors
            else:
                color_list = get_colors_from_colors(colors)
        else:
            raise ValueError("Could not construct ColorMap from the given colors", colors)

        if isinstance(breaks, list):
            return ColorMap.from_colors(breaks, color_list, no_data_color, fallback, classification_strategy)
        elif isinstance(breaks, Histogram):
            return ColorMap.from_histogram(breaks, color_list, no_data_color, fallback, classification_strategy)
        else:
            raise ValueError("Could not construct ColorMap from the given breaks", breaks)

    @classmethod
    def from_break_map(cls, break_map,
                       no_data_color=0x00000000, fallback=0x00000000,
                       classification_strategy=ClassificationStrategy.LESS_THAN_OR_EQUAL_TO):
        """Converts a dictionary mapping from tile values to colors to a ColorMap.

        Args:
            break_map (dict): A mapping from tile values to colors, the latter
                represented as integers e.g., 0xff000080 is red at half opacity.
            no_data_color(int, optional): A color to replace NODATA values with
            fallback (int, optional): A color to replace cells that have no
                value in the mapping
            classification_strategy (str or :class:`~geopyspark.geotrellis.constants.ClassificationStrategy`, optional):
                A string giving the strategy for converting tile values to colors. e.g., if
                ``ClassificationStrategy.LESS_THAN_OR_EQUAL_TO`` is specified, and the break map is
                {3: 0xff0000ff, 4: 0x00ff00ff}, then values up to 3 map to red, values from above 3
                and up to and including 4 become green, and values over 4 become the fallback color.

        Returns:
            :class:`~geopyspark.geotrellis.color.ColorMap`
        """

        pysc = get_spark_context()

        if all(isinstance(x, int) for x in break_map.keys()):
            fn = pysc._gateway.jvm.geopyspark.geotrellis.ColorMapUtils.fromMap
            strat = ClassificationStrategy(classification_strategy).value
            return cls(fn(break_map, no_data_color, fallback, strat))
        elif all(isinstance(x, float) for x in break_map.keys()):
            fn = pysc._gateway.jvm.geopyspark.geotrellis.ColorMapUtils.fromMapDouble
            strat = ClassificationStrategy(classification_strategy).value
            return cls(fn(break_map, no_data_color, fallback, strat))
        else:
            raise TypeError("Break map keys must be either int or float.")

    @classmethod
    def from_colors(cls, breaks, color_list,
                    no_data_color=0x00000000, fallback=0x00000000,
                    classification_strategy=ClassificationStrategy.LESS_THAN_OR_EQUAL_TO):
        """Converts lists of values and colors to a ``ColorMap``.

        Args:
            breaks (list): The tile values that specify breaks in the color
                mapping.
            color_list ([int]): The colors corresponding to the values in the
                breaks list, represented as integers---e.g., 0xff000080 is red
                at half opacity.
            no_data_color(int, optional): A color to replace NODATA values with
            fallback (int, optional): A color to replace cells that have no
                value in the mapping
            classification_strategy (str or :class:`~geopyspark.geotrellis.constants.ClassificationStrategy`, optional):
                A string giving the strategy for converting tile values to colors. e.g., if
                ``ClassificationStrategy.LESS_THAN_OR_EQUAL_TO`` is specified, and the break map is
                {3: 0xff0000ff, 4: 0x00ff00ff}, then values up to 3 map to red, values from above 3
                and up to and including 4 become green, and values over 4 become the fallback color.

        Returns:
            :class:`~geopyspark.geotrellis.color.ColorMap`
        """

        pysc = get_spark_context()

        if all(isinstance(x, int) for x in breaks):
            fn = pysc._gateway.jvm.geopyspark.geotrellis.ColorMapUtils.fromBreaks
            strat = ClassificationStrategy(classification_strategy).value
            return cls(fn(breaks, color_list, no_data_color, fallback, strat))
        else:
            fn = pysc._gateway.jvm.geopyspark.geotrellis.ColorMapUtils.fromBreaksDouble
            arr = [float(br) for br in breaks]
            strat = ClassificationStrategy(classification_strategy).value
            return cls(fn(arr, color_list, no_data_color, fallback, strat))

    @classmethod
    def from_histogram(cls, histogram, color_list,
                       no_data_color=0x00000000, fallback=0x00000000,
                       classification_strategy=ClassificationStrategy.LESS_THAN_OR_EQUAL_TO):
        """Converts a wrapped GeoTrellis histogram into a ``ColorMap``.

        Args:
            histogram (:class:`~geopyspark.geotrellis.Histogram`): A ``Histogram`` instance;
                specifies breaks
            color_list ([int]): The colors corresponding to the values in the
                breaks list, represented as integers e.g., 0xff000080 is red
                at half opacity.
            no_data_color(int, optional): A color to replace NODATA values with
            fallback (int, optional): A color to replace cells that have no
                value in the mapping
            classification_strategy (str or :class:`~geopyspark.geotrellis.constants.ClassificationStrategy`, optional):
                A string giving the strategy for converting tile values to colors. e.g., if
                ``ClassificationStrategy.LESS_THAN_OR_EQUAL_TO`` is specified, and the break map is
                {3: 0xff0000ff, 4: 0x00ff00ff}, then values up to 3 map to red, values from above 3
                and up to and including 4 become green, and values over 4 become the fallback color.

        Returns:
            :class:`~geopyspark.geotrellis.color.ColorMap`
        """

        pysc = get_spark_context()

        fn = pysc._gateway.jvm.geopyspark.geotrellis.ColorMapUtils.fromHistogram
        strat = ClassificationStrategy(classification_strategy).value
        return cls(fn(histogram.scala_histogram, color_list, no_data_color, fallback, strat))

    @staticmethod
    def nlcd_colormap():
        """Returns a color map for NLCD tiles.

        Returns:
            :class:`~geopyspark.geotrellis.color.ColorMap`
        """

        return ColorMap.from_break_map(nlcd_color_map)
