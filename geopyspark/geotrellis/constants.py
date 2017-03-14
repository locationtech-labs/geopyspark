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
