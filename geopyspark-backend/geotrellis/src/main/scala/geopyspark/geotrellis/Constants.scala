package geopyspark.geotrellis

object Constants {
  final val SPATIALKEY = "SpatialKey"
  final val SPACETIMEKEY = "SpaceTimeKey"

  final val PROJECTEDEXTENT = "ProjectedExtent"
  final val TEMPORALPROJECTEDEXTENT = "TemporalProjectedExtent"

  final val S3 = "s3"

  final val FLOAT = "float"
  final val ZOOM = "zoom"

  final val NEARESTNEIGHBOR = "NearestNeighbor"
  final val BILINEAR = "Bilinear"
  final val CUBICCONVOLUTION = "CubicConvolution"
  final val CUBICSPLINE = "CubicSpline"
  final val LANCZOS = "Lanczos"
  final val AVERAGE = "Average"
  final val MODE = "Mode"
  final val MEDIAN = "Median"
  final val MAX = "Max"
  final val MIN = "Min"

  final val MEAN = "Mean"
  final val SUM = "Sum"
  final val STANDARDDEVIATION = "StandardDeviation"
  final val SLOPE = "Slope"
  final val ASPECT = "Aspect"

  final val ANNULUS = "Annulus"
  final val NESW = "Nesw"
  final val SQUARE = "Square"
  final val WEDGE = "Wedge"
  final val CIRCLE = "Circle"

  final val GREATERTHAN = "GreaterThan"
  final val GREATERTHANOREQUALTO = "GreaterThanOrEqualTo"
  final val LESSTHAN = "LessThan"
  final val LESSTHANOREQUALTO = "LessThanOrEqualTo"
  final val EXACT = "Exact"

  final val NOCOMPRESSION = "NoCompression"
  final val DEFLATECOMPRESSION = "DeflateCompression"

  final val STRIPED = "Striped"
  final val TILED = "Tiled"
}
