"""Constants that are used by ``geopyspark.geotrellis`` classes, methods, and functions."""

"""
Indicates that the RDD contains ``(K, V)`` pairs, where the ``K`` has a spatial attribute,
but no time value. Both :ref:`projected_extent` and :ref:`spatial-key`` are examples of this
type of ``K``.
"""
SPATIAL = 'spatial'

"""
Indicates that the RDD contains ``(K, V)`` pairs, where the ``K`` has a spatial and
time attribute. Both :ref:`temporal_extent` and :ref:`space-time-key` are examples
of this type of ``K``.
"""
SPACETIME = 'spacetime'


"""
Indicates the type value that needs to be serialized/deserialized. Both singleband
and multiband GeoTiffs are referred to as this.
"""
TILE = 'Tile'


"""A resampling method."""
NEARESTNEIGHBOR = 'NearestNeighbor'

"""A resampling method."""
BILINEAR = 'Bilinear'

"""A resampling method."""
CUBICCONVOLUTION = 'CubicConvolution'

"""A resampling method."""
CUBICSPLINE = 'CubicSpline'

"""A resampling method."""
LANCZOS = 'Lanczos'

"""A resampling method."""
AVERAGE = 'Average'

"""A resampling method."""
MODE = 'Mode'

"""A resampling method."""
MEDIAN = 'Median'

"""A resampling method."""
MAX = 'Max'

"""A resampling method."""
MIN = 'Min'

RESAMPLE_METHODS = [
    NEARESTNEIGHBOR,
    BILINEAR,
    CUBICCONVOLUTION,
    LANCZOS,
    AVERAGE,
    MODE,
    MEDIAN,
    MAX,
    MIN
]


"""Layout scheme to match resolution of the closest level of TMS pyramid."""
ZOOM = 'zoom'

"""Layout scheme to match resolution of source rasters."""
FLOAT = 'float'


"""A key indexing method. Works for RDD that contain both :ref:`spatial-key` and
:ref:`space-time-key`.
"""
ZORDER = 'zorder'

"""
A key indexing method. Works for RDDs that contain both :ref:`spatial-key` and
:ref:`space-time-key`. Note, indexes are determined by the ``x``, ``y``, and if
``SPACETIME``, the temporal resolutions of a point. This is expressed in bits, and
has a max value of 62. Thus if the sum of those resolutions are greater than 62,
then the indexing will fail.
"""
HILBERT = 'hilbert'

"""A key indexing method. Works only for RDDs that contain :ref:`spatial-key`.
This method provides the fastest lookup of all the key indexing method, however, it does not give
good locality guarantees. It is recommended then that this method should only be used when locality
is not important for your analysis.
"""
ROWMAJOR = 'rowmajor'


"""A time unit used with ZORDER."""
MILLISECONDS = 'millis'

"""A time unit used with ZORDER."""
SECONDS = 'seconds'

"""A time unit used with ZORDER."""
MINUTES = 'minutes'

"""A time unit used with ZORDER."""
HOURS = 'hours'

"""A time unit used with ZORDER."""
DAYS = 'days'

"""A time unit used with ZORDER."""
MONTHS = 'months'

"""A time unit used with ZORDER."""
YEARS = 'years'


"""Neighborhood type."""
ANNULUS = 'annulus'

"""Neighborhood type."""
NESW = 'nesw'

"""Neighborhood type."""
SQUARE = 'square'

"""Neighborhood type."""
WEDGE = 'wedge'

"""Neighborhood type."""
CIRCLE = "circle"

"""Focal operation type."""
SUM = 'Sum'

"""Focal operation type."""
MEAN = 'Mean'

"""Focal operation type"""
ASPECT = 'Aspect'

"""Focal operation type."""
SLOPE = 'Slope'

"""Focal operation type."""
STANDARDDEVIATION = 'StandardDeviation'

OPERATIONS = [
    SUM,
    MIN,
    MAX,
    MEAN,
    MEDIAN,
    MODE,
    STANDARDDEVIATION,
    ASPECT,
    SLOPE
]

NEIGHBORHOODS = [
    ANNULUS,
    NESW,
    SQUARE,
    WEDGE,
    CIRCLE
]

"""The NoData value for ints in GeoTrellis."""
NODATAINT = -2147483648

"""A classification strategy."""
GREATERTHAN = "GreaterThan"

"""A classification strategy."""
GREATERTHANOREQUALTO = "GreaterThanOrEqualTo"

"""A classification strategy."""
LESSTHAN = "LessThan"

"""A classification strategy."""
LESSTHANOREQUALTO = "LessThanOrEqualTo"

"""A classification strategy."""
EXACT = "Exact"


"""Representes Bit Cells."""
BOOLRAW = "boolraw"

"""Representes Byte Cells."""
INT8RAW = "int8raw"

"""Representes UByte Cells."""
UINT8RAW = "uint8raw"

"""Representes Short Cells."""
INT16RAW = "int16raw"

"""Representes UShort Cells."""
UINT16RAW = "uint16raw"

"""Representes Int Cells."""
INT32RAW = "int32raw"

"""Representes Float Cells."""
FLOAT32RAW = "float32raw"

"""Representes Double Cells."""
FLOAT64RAW = "float64raw"

"""Representes Bit Cells."""
BOOL = "bool"

"""Representes Byte Cells with constant NoData values."""
INT8 = "int8"

"""Representes UByte Cells with constant NoData values."""
UINT8 = "uint8"

"""Representes Short Cells with constant NoData values."""
INT16 = "int16"

"""Representes UShort Cells with constant NoData values."""
UINT16 = "uint16"

"""Representes Int Cells with constant NoData values."""
INT32 = "int32"

"""Representes Float Cells with constant NoData values."""
FLOAT32 = "float32"

"""Representes Double Cells with constant NoData values."""
FLOAT64 = "float64"

def create_no_data_constant(cell_type, no_data_value):
    """Creates a ``CellType`` that has a user defined NoData value.

    Cannot be used when the ``cell_type`` is either a boolean or contains raw values.

    Args:
        cell_type (str): The string representation of the ``CellType`` to convert to. It is
            represented by a constant such as ``INT16``, ``FLOAT64``, etc.
        no_data_value (int or float): The value that should be marked as NoData.

    Returns:
        A user defined noData constant CellType (str)

    Raises:
        ValueError: If ``cell_type`` is a boolean.
        ValueError: If the ``cell_type`` contains raw values.
        ValueError: If the ``cell_type`` is not a known ``CellType``.
    """

    if 'bool' in cell_type:
        raise ValueError("Cannot add user defined types to Bool")
    elif 'raw' in cell_type:
        raise ValueError("Cannot add user defined types to raw values")
    elif cell_type not in CELL_TYPES:
        raise ValueError(cell_type, "Is not a known CellType")
    else:
        no_data_constant = cell_type + "ud" + str(no_data_value)
        CELL_TYPES.append(no_data_constant)

        return no_data_constant


CELL_TYPES = [
    BOOLRAW,
    INT8RAW,
    UINT8RAW,
    INT16RAW,
    UINT16RAW,
    INT32RAW,
    FLOAT32RAW,
    FLOAT64RAW,
    BOOL,
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    FLOAT32,
    FLOAT64
]


"""A ColorRamp."""
HOT = "hot"

"""A ColorRamp."""
COOLWARM = "coolwarm"

"""A ColorRamp."""
MAGMA = "magma"

"""A ColorRamp."""
INFERNO = "inferno"

"""A ColorRamp."""
PLASMA = "plasma"

"""A ColorRamp."""
VIRIDIS = "viridis"

"""A ColorRamp."""
BLUE_TO_ORANGE = "BlueToOrange"

"""A ColorRamp."""
LIGHT_YELLOW_TO_ORANGE = "LightYellowToOrange"

"""A ColorRamp."""
BLUE_TO_RED = "BlueToRed"

"""A ColorRamp."""
GREEN_TO_RED_ORANGE = "GreenToRedOrange"

"""A ColorRamp."""
LIGHT_TO_DARK_SUNSET = "LightToDarkSunset"

"""A ColorRamp."""
LIGHT_TO_DARK_GREEN = "LightToDarkGreen"

"""A ColorRamp."""
HEATMAP_YELLOW_TO_RED = "HeatmapYellowToRed"

"""A ColorRamp."""
HEATMAP_BLUE_TO_YELLOW_TO_RED_SPECTRUM = "HeatmapBlueToYellowToRedSpectrum"

"""A ColorRamp."""
HEATMAP_DARK_RED_TO_YELLOW_WHITE = "HeatmapDarkRedToYellowWhite"

"""A ColorRamp."""
HEATMAP_LIGHT_PURPLE_TO_DARK_PURPLE_TO_WHITE = "HeatmapLightPurpleToDarkPurpleToWhite"

"""A ColorRamp."""
CLASSIFICATION_BOLD_LAND_USE = "ClassificationBoldLandUse"

"""A ColorRamp."""
CLASSIFICATION_MUTED_TERRAIN = "ClassificationMutedTerrain"

COLOR_RAMPS = [
    HOT,
    COOLWARM,
    MAGMA,
    INFERNO,
    PLASMA,
    VIRIDIS,
    BLUE_TO_ORANGE,
    LIGHT_YELLOW_TO_ORANGE,
    BLUE_TO_RED,
    GREEN_TO_RED_ORANGE,
    LIGHT_TO_DARK_SUNSET,
    LIGHT_TO_DARK_GREEN,
    HEATMAP_YELLOW_TO_RED,
    HEATMAP_BLUE_TO_YELLOW_TO_RED_SPECTRUM,
    HEATMAP_DARK_RED_TO_YELLOW_WHITE,
    HEATMAP_LIGHT_PURPLE_TO_DARK_PURPLE_TO_WHITE,
    CLASSIFICATION_BOLD_LAND_USE,
    CLASSIFICATION_MUTED_TERRAIN
]
