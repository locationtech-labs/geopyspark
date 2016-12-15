from pyspark import SparkConf, SparkContext
from py4j.java_gateway import java_import

if __name__ == "__main__":
    sc = SparkContext(master="local", appName="python-test")

    java_import(sc._gateway.jvm, "geotrellis.spark.io.hadoop.*")
    path = "../../econic.tif"

    hssgr = sc._gateway.jvm.HadoopSpatialSinglebandGeoTiffRDD(path, sc._jsc)
    crs = hssgr.fromEpsgCode(3355)

    original_splits = hssgr.split(258, 258)
    #print ' THESE ARE THE SPLITS !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1'
    #print original_splits

    sc._gateway.jvm.original_splits.map(lambda x: x)
    reprojected_splits = hssgr.reproject(original_splits, crs)

    original_value = original_splits.first()
    reprojected_value = reprojected_splits.first()

    print original_value._1(), reprojected_value._1()
