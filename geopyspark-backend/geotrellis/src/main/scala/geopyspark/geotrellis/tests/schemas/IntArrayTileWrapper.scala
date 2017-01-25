package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object IntArrayTileWrapper extends Wrapper[IntArrayTile] {

  def testRdd(sc: SparkContext): RDD[IntArrayTile] = {
    val arr = Array(
      IntArrayTile(Array[Int](0, 0, 1, 1), 2, 2),
      IntArrayTile(Array[Int](1, 2, 3, 4), 2, 2),
      IntArrayTile(Array[Int](5, 6, 7, 8), 2, 2))
    sc.parallelize(arr)
  }
}
