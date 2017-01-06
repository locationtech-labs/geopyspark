#!/bin/env python3

from pyspark import SparkConf, SparkContext, RDD
# from pyspark.serializers import Serializer, FramedSerializer, AutoBatchedSerializer
# from py4j.java_gateway import java_import
# from geopyspark.avroserializer import AvroSerializer
# from geopyspark.tile import TileArray

# import io
# import sys
# import numpy as np


if __name__ == "__main__":
    sc = SparkContext(appName="layer-test")

    factory = sc._gateway.jvm.geopyspark.geotrellis.io.AttributeStoreFactory
    store = factory.build("hdfs", "file:/tmp/catalog-cache", sc._jsc.sc())
    header = store.header("ned", 0)
    cell_type = store.cellType("ned", 0)
    print([header[i] for i in range(len(header))])
    print(cell_type)

    # (arrays, schema) = get_rdd(sc)

    # adder = lambda x: x + 1
    # new_arrays = arrays.map(lambda s: TileArray(np.array([adder(x1) for x1 in s]), s.no_data_value))

    # set_rdd(sc, new_arrays, schema)
