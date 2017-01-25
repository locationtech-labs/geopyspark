package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object ShortArrayTileWrapper extends Wrapper[ShortArrayTile] {

  def testRdd(sc: SparkContext): RDD[ShortArrayTile] = {
    val arr = Array(
      ShortArrayTile(Array[Short](0, 0, 1, 1), 2, 2),
      ShortArrayTile(Array[Short](1, 2, 3, 4), 2, 2),
      ShortArrayTile(Array[Short](5, 6, 7, 8), 2, 2))
    sc.parallelize(arr)
  }
}
