package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object SpatialKeyWrapper extends Wrapper[SpatialKey] {
  def testRdd(sc: SparkContext): RDD[SpatialKey] = {
    val arr = Array(
      SpatialKey(7,3))
    sc.parallelize(arr)
  }
}
