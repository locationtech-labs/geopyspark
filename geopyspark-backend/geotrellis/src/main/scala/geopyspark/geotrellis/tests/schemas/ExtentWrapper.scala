package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.vector.Extent
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object ExtentWrapper extends Wrapper[Extent]{

  def testRdd(sc: SparkContext): RDD[Extent] = {
    val arr = Array(
      Extent(0, 0, 1, 1),
      Extent(1, 2, 3, 4),
      Extent(5, 6, 7, 8))
    sc.parallelize(arr)
  }
}
