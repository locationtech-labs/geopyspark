package geopyspark.geotrellis

import geotrellis.proj4._
import geotrellis.vector.Extent
import geotrellis.spark._
import geotrellis.spark.io._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object TemporalProjectedExtentWrapper extends Wrapper[TemporalProjectedExtent]{

  def testRdd(sc: SparkContext): RDD[TemporalProjectedExtent] = {
    val arr = Array(
      TemporalProjectedExtent(Extent(0, 0, 1, 1), CRS.fromEpsgCode(2004), 0.toLong),
      TemporalProjectedExtent(Extent(1, 2, 3, 4), CRS.fromEpsgCode(2004), 1.toLong),
      TemporalProjectedExtent(Extent(5, 6, 7, 8), CRS.fromEpsgCode(2004), 2.toLong))
    sc.parallelize(arr)
  }
}
