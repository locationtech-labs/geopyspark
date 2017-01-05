package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object BitArrayTileWrapper extends Wrapper[BitArrayTile] {

  def testRdd(sc: SparkContext): RDD[BitArrayTile] = {
    val arr = Array(
      BitArrayTile(Array[Byte](0, 0, 1, 1), 2, 2),
      BitArrayTile(Array[Byte](1, 2, 3, 4), 2, 2),
      BitArrayTile(Array[Byte](5, 6, 7, 8), 2, 2))
    println("\n\n\n")
    println("THESE ARE THE ORIGINAL BITARRAYTILES")
    arr.foreach(println)
    println("\n\n\n")
    sc.parallelize(arr)
  }
}
