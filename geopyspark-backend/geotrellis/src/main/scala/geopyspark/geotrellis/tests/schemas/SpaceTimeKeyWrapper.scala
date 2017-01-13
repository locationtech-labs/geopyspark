package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object SpaceTimeKeyWrapper extends Wrapper[SpaceTimeKey] {
  def testRdd(sc: SparkContext): RDD[SpaceTimeKey] = {
    val arr = Array(
      SpaceTimeKey(7, 3, 5.toLong),
      SpaceTimeKey(9, 4, 10.toLong),
      SpaceTimeKey(11, 5, 15.toLong))
    sc.parallelize(arr)
  }
}
