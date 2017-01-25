from pyspark import SparkConf, SparkContext, RDD
from geopyspark.s3_geotiff_rdd import S3GeoTiffRDD

if __name__ == "__main__":
    bucket = "gt-rasters"
    prefix = "nlcd/2011/tiles-small/nlcd_2011_01_01.tif"
    options = {'maxTileSize': 256, 'chunkSize': 256}

    pysc = SparkContext(master="local", appName="s3-geotiff-test")

    s3_geotiff = S3GeoTiffRDD(pysc)

    rdds = s3_geotiff.spatial(bucket, prefix)

    result = rdds.first()

    print(result)
