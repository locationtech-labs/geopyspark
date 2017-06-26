"""Constants that are used by ``geopyspark.geotrellis`` classes, methods, and functions."""

"""
Indicates that the RDD contains ``(K, V)`` pairs, where the ``K`` has a spatial attribute,
but no time value. Both :class:`~geopyspark.geotrellis.ProjectedExtent` and
:class:`~geopyspark.geotrellis.SpatialKey` are examples of this type of ``K``.
"""
SPATIAL = 'spatial'

"""
Indicates that the RDD contains ``(K, V)`` pairs, where the ``K`` has a spatial and
time attribute. Both :class:`~geopyspark.geotrellis.TemporalProjectedExtent`
and :class:`~geopyspark.geotrellis.SpaceTimeKey` are examples of this type of ``K``.
"""
SPACETIME = 'spacetime'

"""Layout scheme to match resolution of the closest level of TMS pyramid."""
ZOOM = 'zoom'

"""Layout scheme to match resolution of source rasters."""
FLOAT = 'float'


"""A key indexing method. Works for RDD that contain both :class:`~geopyspark.geotrellis.SpatialKey`
and :class:`~geopyspark.geotrellis.SpaceTimeKey`.
"""
ZORDER = 'zorder'

"""
A key indexing method. Works for RDDs that contain both :class:`~geopyspark.geotrellis.SpatialKey`
and :class:`~geopyspark.geotrellis.SpaceTimeKey`. Note, indexes are determined by the ``x``,
``y``, and if ``SPACETIME``, the temporal resolutions of a point. This is expressed in bits, and
has a max value of 62. Thus if the sum of those resolutions are greater than 62,
then the indexing will fail.
"""
HILBERT = 'hilbert'

"""A key indexing method. Works only for RDDs that contain :class:`~geopyspark.geotrellis.SpatialKey`.
This method provides the fastest lookup of all the key indexing method, however, it does not give
good locality guarantees. It is recommended then that this method should only be used when locality
is not important for your analysis.
"""
ROWMAJOR = 'rowmajor'

"""The NoData value for ints in GeoTrellis."""
NODATAINT = -2147483648


class ResampleMethods(object):
    """Resampling Methods."""

    NEARESTNEIGHBOR = 'NearestNeighbor'
    BILINEAR = 'Bilinear'
    CUBICCONVOLUTION = 'CubicConvolution'
    CUBICSPLINE = 'CubicSpline'
    LANCZOS = 'Lanczos'
    AVERAGE = 'Average'
    MODE = 'Mode'
    MEDIAN = 'Median'
    MAX = 'Max'
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


class TimeUnits(object):
    """ZORDER time units."""

    MILLISECONDS = 'millis'
    SECONDS = 'seconds'
    MINUTES = 'minutes'
    HOURS = 'hours'
    DAYS = 'days'
    MONTHS = 'months'
    YEARS = 'years'

    TIME_UNITS = [
        MILLISECONDS,
        SECONDS,
        MINUTES,
        HOURS,
        DAYS,
        MONTHS,
        YEARS
    ]


class Operation(object):
    """Focal opertions."""

    SUM = 'Sum'
    MEAN = 'Mean'
    MODE = 'Mode'
    MEDIAN = 'Median'
    MAX = 'Max'
    MIN = 'Min'
    ASPECT = 'Aspect'
    SLOPE = 'Slope'
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


class Neighborhood(object):
    """Neighborhood types."""

    ANNULUS = 'annulus'
    NESW = 'nesw'
    SQUARE = 'square'
    WEDGE = 'wedge'
    CIRCLE = "circle"

    NEIGHBORHOODS = [
        ANNULUS,
        NESW,
        SQUARE,
        WEDGE,
        CIRCLE
    ]


class ClassificationStrategies(object):
    """Classification strategies for color mapping."""

    GREATERTHAN = "GreaterThan"
    GREATERTHANOREQUALTO = "GreaterThanOrEqualTo"
    LESSTHAN = "LessThan"
    LESSTHANOREQUALTO = "LessThanOrEqualTo"
    EXACT = "Exact"

    CLASSIFCATION_STRATEGIES = [
        GREATERTHAN,
        GREATERTHANOREQUALTO,
        LESSTHAN,
        LESSTHANOREQUALTO,
        EXACT
    ]


class CellTypes(object):
    """Cell types."""

    BOOLRAW = "boolraw"
    INT8RAW = "int8raw"
    UINT8RAW = "uint8raw"
    INT16RAW = "int16raw"
    UINT16RAW = "uint16raw"
    INT32RAW = "int32raw"
    FLOAT32RAW = "float32raw"
    FLOAT64RAW = "float64raw"
    BOOL = "bool"
    INT8 = "int8"
    UINT8 = "uint8"
    INT16 = "int16"
    UINT16 = "uint16"
    INT32 = "int32"
    FLOAT32 = "float32"
    FLOAT64 = "float64"

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


class ColorRamps(object):
    """ColorRamp names."""

    HOT = "hot"
    COOLWARM = "coolwarm"
    MAGMA = "magma"
    INFERNO = "inferno"
    PLASMA = "plasma"
    VIRIDIS = "viridis"
    BLUE_TO_ORANGE = "BlueToOrange"
    LIGHT_YELLOW_TO_ORANGE = "LightYellowToOrange"
    BLUE_TO_RED = "BlueToRed"
    GREEN_TO_RED_ORANGE = "GreenToRedOrange"
    LIGHT_TO_DARK_SUNSET = "LightToDarkSunset"
    LIGHT_TO_DARK_GREEN = "LightToDarkGreen"
    HEATMAP_YELLOW_TO_RED = "HeatmapYellowToRed"
    HEATMAP_BLUE_TO_YELLOW_TO_RED_SPECTRUM = "HeatmapBlueToYellowToRedSpectrum"
    HEATMAP_DARK_RED_TO_YELLOW_WHITE = "HeatmapDarkRedToYellowWhite"
    HEATMAP_LIGHT_PURPLE_TO_DARK_PURPLE_TO_WHITE = "HeatmapLightPurpleToDarkPurpleToWhite"
    CLASSIFICATION_BOLD_LAND_USE = "ClassificationBoldLandUse"
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
