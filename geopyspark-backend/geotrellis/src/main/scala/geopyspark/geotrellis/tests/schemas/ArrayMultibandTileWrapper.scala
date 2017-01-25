package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis.testkit._
import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object ArrayMultibandTileWrapper extends Wrapper[MultibandTile] {

  def testRdd(sc: SparkContext): RDD[MultibandTile] = {
    val tile = ByteArrayTile(Array[Byte](0, 0, 1, 1), 2, 2)

    val multi = Array(ArrayMultibandTile(tile, tile, tile),
      ArrayMultibandTile(tile, tile, tile),
      ArrayMultibandTile(tile, tile, tile))

    sc.parallelize(multi)
  }
}
