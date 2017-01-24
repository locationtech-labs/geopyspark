package geopyspark.geotrellis

import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.spark._
import geotrellis.spark.io._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object ProjectedExtentWrapper extends Wrapper[ProjectedExtent]{

  def testRdd(sc: SparkContext): RDD[ProjectedExtent] = {
    val arr = Array(
      ProjectedExtent(Extent(0, 0, 1, 1), CRS.fromEpsgCode(2004)),
      ProjectedExtent(Extent(1, 2, 3, 4), CRS.fromEpsgCode(2004)),
      ProjectedExtent(Extent(5, 6, 7, 8), CRS.fromEpsgCode(2004)))
    sc.parallelize(arr)
  }
}
