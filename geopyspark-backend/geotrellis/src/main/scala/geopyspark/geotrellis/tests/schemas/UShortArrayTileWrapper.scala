package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object UShortArrayTileWrapper extends Wrapper[UShortArrayTile] {

  def testRdd(sc: SparkContext): RDD[UShortArrayTile] = {
    val arr = Array(
      UShortArrayTile(Array[Short](0, 0, 1, 1), 2, 2),
      UShortArrayTile(Array[Short](1, 2, 3, 4), 2, 2),
      UShortArrayTile(Array[Short](5, 6, 7, 8), 2, 2))
    sc.parallelize(arr)
  }
}
