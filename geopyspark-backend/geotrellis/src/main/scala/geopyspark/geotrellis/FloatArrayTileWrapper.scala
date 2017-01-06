package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object FloatArrayTileWrapper extends Wrapper[FloatArrayTile] {

  def testRdd(sc: SparkContext): RDD[FloatArrayTile] = {
    val arr = Array(
      FloatArrayTile(Array[Float](0, 0, 1, 1), 2, 2),
      FloatArrayTile(Array[Float](1, 2, 3, 4), 2, 2),
      FloatArrayTile(Array[Float](5, 6, 7, 8), 2, 2))
    println("\n\n\n")
    println("THESE ARE THE ORIGINAL FLOATARRAYTILES")
    arr.foreach(println)
    println("\n\n\n")
    sc.parallelize(arr)
  }
}

