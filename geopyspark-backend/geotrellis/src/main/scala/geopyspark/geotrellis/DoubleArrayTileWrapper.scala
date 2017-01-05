package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object DoubleArrayTileWrapper extends Wrapper[DoubleArrayTile] {

  def testRdd(sc: SparkContext): RDD[DoubleArrayTile] = {
    val arr = Array(
      DoubleArrayTile(Array[Double](0, 0, 1, 1), 2, 2),
      DoubleArrayTile(Array[Double](1, 2, 3, 4), 2, 2),
      DoubleArrayTile(Array[Double](5, 6, 7, 8), 2, 2))
    println("\n\n\n")
    println("THESE ARE THE ORIGINAL DOUBLEARRAYTILES")
    arr.foreach(println)
    println("\n\n\n")
    sc.parallelize(arr)
  }
}

