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

"""A resampling methods"""
NEARESTNEIGHBOR = 'NearestNeighbor'

"""A resampling methods"""
BILINEAR = 'Bilinear'

"""A resampling methods"""
CUBICCONVOLUTION = 'CubicConvolution'

"""A resampling methods"""
CUBICSPLINE = 'CubicSpline'

"""A resampling methods"""
LANCZOS = 'Lanczos'

"""A resampling methods"""
AVERAGE = 'Average'

"""A resampling methods"""
MODE = 'Mode'

"""A resampling methods"""
MEDIAN = 'Median'

"""A resampling methods"""
MAX = 'Max'

"""A resampling methods"""
MIN = 'Min'
