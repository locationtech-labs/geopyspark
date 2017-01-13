package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.raster._
import geotrellis.vector.Extent
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object TupleWrapper extends Wrapper[(ByteArrayTile, Extent)]{

  def testRdd(sc: SparkContext): RDD[(ByteArrayTile, Extent)] = {
    val arr = Array(
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 2, 3), Extent(0, 0, 1, 1)),
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 3, 2), Extent(1, 2, 3, 4)),
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 1, 6), Extent(5, 6, 7, 8)))
    sc.parallelize(arr)
  }
}
