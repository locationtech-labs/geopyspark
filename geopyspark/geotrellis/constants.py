"""
Indicates that the RDD contains (K, V) pairs, where the K has a spatial attribute,
but no time value. Both projected_extent and spatial_key are examples of this
type of K.
"""
SPATIAL = 'spatial'

"""
Indicates that the RDD contains (K, V) pairs, where the K has a spatial and
time attribute. Both temporal_projected_extent and spacetime_key are examples
of this type of K.
"""
SPACETIME = 'spacetime'

"""
Indicates the type value that needs to be serialized/deserialized. Both singleband
and multiband GeoTiffs are reffered to as this.
"""
TILE = 'Tile'

"""A resampling method"""
NEARESTNEIGHBOR = 'NearestNeighbor'

"""A resampling method"""
BILINEAR = 'Bilinear'

"""A resampling method"""
CUBICCONVOLUTION = 'CubicConvolution'

"""A resampling method"""
CUBICSPLINE = 'CubicSpline'

"""A resampling method"""
LANCZOS = 'Lanczos'

"""A resampling method"""
AVERAGE = 'Average'

"""A resampling method"""
MODE = 'Mode'

"""A resampling method"""
MEDIAN = 'Median'

"""A resampling method"""
MAX = 'Max'

"""A resampling method"""
MIN = 'Min'
