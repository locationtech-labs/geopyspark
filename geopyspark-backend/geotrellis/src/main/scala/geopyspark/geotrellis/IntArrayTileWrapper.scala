package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object IntArrayTileWrapper extends Wrapper[IntArrayTile] {

  def testRdd(sc: SparkContext): RDD[IntArrayTile] = {
    val arr = Array(
      IntArrayTile(Array[Byte](0, 0, 1, 1), 2, 2),
      IntArrayTile(Array[Byte](1, 2, 3, 4), 2, 2),
      IntArrayTile(Array[Byte](5, 6, 7, 8), 2, 2))
    println("\n\n\n")
    println("THESE ARE THE ORIGINAL INTARRAYTILES")
    arr.foreach(println)
    println("\n\n\n")
    sc.parallelize(arr)
  }
}

