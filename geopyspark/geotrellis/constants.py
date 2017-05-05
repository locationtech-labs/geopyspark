"""Constants that are used by geopyspark.geotrellis classes, methods, and functions."""

"""
Indicates that the RDD contains (K, V) pairs, where the K has a spatial attribute,
but no time value. Both ProjectedExtent and SpatialKey are examples of this
type of K.
"""
SPATIAL = 'spatial'

"""
Indicates that the RDD contains (K, V) pairs, where the K has a spatial and
time attribute. Both TemporalProjectedExtent and SpaceTimeKey are examples
of this type of K.
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


"""Layout scheme to match resolution of the closest level of TMS pyramid"""
ZOOM = 'zoom'

"""Layout scheme to match resolution of source rasters"""
FLOAT = 'float'


"""A key indexing method. Works for RDDs that contain both SpatialKeys and SpacetimeKeys."""
ZORDER = 'zorder'

"""
A key indexing method. Works for RDDs that contain both SpatialKeys and SpacetimeKeys.
Note, indexes are determined by the x, y, and if SPACETIME, the temporal resolutions of
a point. This is expressed in bits, and has a max value of 62. Thus if the sum of those
resolutions are greater than 62, then the indexing will fail
"""
HILBERT = 'hilbert'

"""A key indexing method. Works only for RDDs that contain SpatialKeys.
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
