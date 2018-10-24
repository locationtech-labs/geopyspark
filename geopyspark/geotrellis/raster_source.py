from geopyspark import get_spark_context
from geopyspark.geopyspark_utils import ensure_pyspark
ensure_pyspark()

from geopyspark.geotrellis import crs_to_proj4, LocalLayout, SpaceTimePartitionStrategy

from geopyspark.geotrellis.constants import (LayerType,
                                             ResampleMethod,
                                             ReadMethod)

from geopyspark.geotrellis.layer import TiledRaterLayer

def read_to_layout(layer_type=LayerType.SPATIAL, # The default param should be removed at some point
                   paths, # Could this be a str, [str] or an RDD[str]?
                   layout=LocalLayout(),
                   target_crs=None,
                   resample_method=ResampleMethod.NEAREST_NEIGHBOR,
                   partition_strategy=None,
                   read_method=ReadMethod.GEOTRELLIS):

    pysc = get_spark_context()

    raster_source = pysc._gateway.jvm.geopyspark.geotrellis.vlm.RasterSource

    check_partition_strategy(partition_strategy, layer_type)

    # This should be either be a SpatialKey or a SpaceTimeKey.
    # However, geotrellis-contrib only support spatial data
    # for the time being
    key = LayerType(layer_type)._key_name(False)
    resample_method = ResampleMethod(resample_method)

    read_method = ReadMethod(read_method)

    target_crs = crs_to_proj4(target_crs) or None

    srdd = RasterSource.read(pysc._jsc.sc(),
                             key,
                             paths,
                             layout,
                             target_crs,
                             resample_method,
                             partition_strategy,
                             read_method)

    return TiledRasterLayer(layer_type, srdd)
