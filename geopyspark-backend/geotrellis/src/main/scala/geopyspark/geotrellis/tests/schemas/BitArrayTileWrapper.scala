package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object BitArrayTileWrapper extends Wrapper[BitArrayTile] {

  def testRdd(sc: SparkContext): RDD[BitArrayTile] = {
    val arr = Array(
      BitArrayTile(Array[Byte](0), 1, 1),
      BitArrayTile(Array[Byte](1), 1, 1),
      BitArrayTile(Array[Byte](0), 1, 1))
    sc.parallelize(arr)
  }
}
