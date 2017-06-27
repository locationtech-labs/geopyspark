"""Constants that are used by ``geopyspark.geotrellis`` classes, methods, and functions."""
from enum import Enum


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


class ResampleMethod(Enum):
    """Resampling Methods."""

    NearestNeighbor = 'NearestNeighbor'
    Bilinear = 'Bilinear'
    CubicConvolution = 'CubicConvolution'
    CubicSpline = 'CubicSpline'
    Lanczos = 'Lanczos'
    Average = 'Average'
    Mode = 'Mode'
    Median = 'Median'
    Max = 'Max'
    Min = 'Min'

    RESAMPLE_METHODS = [
        'NearestNeighbor',
        'Bilinear',
        'CubicConvolution',
        'Lanczos',
        'Average',
        'Mode',
        'Median',
        'Max',
        'Min'
    ]


class TimeUnit(Enum):
    """ZORDER time units."""

    millis = 'millis'
    seconds = 'seconds'
    minutes = 'minutes'
    hours = 'hours'
    days = 'days'
    months = 'months'
    years = 'years'

    time_units = [
        'millis',
        'seconds',
        'minutes',
        'hours',
        'days',
        'months',
        'years'
    ]


class Operation(Enum):
    """Focal opertions."""

    Sum = 'Sum'
    Mean = 'Mean'
    Mode = 'Mode'
    Median = 'Median'
    Max = 'Max'
    Min = 'Min'
    Aspect = 'Aspect'
    Slope = 'Slope'
    StandardDeviation = 'StandardDeviation'

    OPERATIONS = [
        'Sum',
        'Min',
        'Max',
        'Mean',
        'Median',
        'Mode',
        'StandardDeviation',
        'Aspect',
        'Slope'
    ]


class Neighborhood(Enum):
    """Neighborhood types."""

    Annulus = 'Annulus'
    Nesw = 'Nesw'
    Square = 'Square'
    Wedge = 'Wedge'
    Circle = "Circle"

    NEIGHBORHOODS = [
        'Annulus',
        'Nesw',
        'Square',
        'Wedge',
        'Circle'
    ]


class ClassificationStrategy(Enum):
    """Classification strategies for color mapping."""

    GreaterThan = "GreaterThan"
    GreaterThanOrEqualTo = "GreaterThanOrEqualTo"
    LessThan = "LessThan"
    LessThanOrEqualTo = "LessThanOrEqualTo"
    Exact = "Exact"

    CLASSIFICATION_STRATEGIES = [
        'GreaterThan',
        'GreaterThanOrEqualTo',
        'LessThan',
        'LessThanOrEqualTo',
        'Exact'
    ]


class CellType(Enum):
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
        'boolraw',
        'int8raw',
        'uint8raw',
        'int16raw',
        'uint16raw',
        'int32raw',
        'float32raw',
        'float64raw',
        'bool',
        'int8',
        'uint8',
        'int16',
        'uint16',
        'int32',
        'float32',
        'float64'
    ]


class ColorRamp(Enum):
    """ColorRamp names."""

    Hot = "Hot"
    CoolWarm = "CoolWarm"
    Magma = "Magma"
    Inferno = "Inferno"
    Plasma = "Plasma"
    Viridis = "Viridis"
    BlueToOrange = "BlueToOrange"
    LightYellowToOrange = "LightYellowToOrange"
    BlueToRed = "BlueToRed"
    GreenToRedOrange = "GreenToRedOrange"
    LightToDarkSunset = "LightToDarkSunset"
    LightToDarkGreen = "LightToDarkGreen"
    HeatmapYellowToRED = "HeatmapYellowToRed"
    HeatmapBlueToYellowToRedSpectrum = "HeatmapBlueToYellowToRedSpectrum"
    HeatmapDarkRedToYellowWhite = "HeatmapDarkRedToYellowWhite"
    HeatmapLightPurpleToDarkPurpleToWhite = "HeatmapLightPurpleToDarkPurpleToWhite"
    ClassificationBoldLandUse = "ClassificationBoldLandUse"
    ClassificationMutedTerrain = "ClassificationMutedTerrain"

    COLOR_RAMPS = [
        'Hot',
        'CoolWarm',
        'Magma',
        'Inferno',
        'Plasma',
        'Viridis',
        'BlueToOrange',
        'LightYellowToOrange',
        'BlueToRed',
        'GreenToRedOrange',
        'LightToDarkSunset',
        'LightToDarkGreen',
        'HeatmapYellowToRed',
        'HeatmapBlueToYellowToRedSpectrum',
        'HeatmapDarkRedToYellowWhite',
        'HeatmapLightPurpleToDarkPurpleToWhite',
        'ClassificationBoldLandUse',
        'ClassificationMutedTerrain'
    ]
