package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD

object ByteArrayTileWrapper extends Wrapper[ByteArrayTile] {

  def testRdd(sc: SparkContext): RDD[ByteArrayTile] = {
    val arr = Array(
      ByteArrayTile(Array[Byte](0, 0, 1, 1), 2, 2),
      ByteArrayTile(Array[Byte](1, 2, 3, 4), 2, 2),
      ByteArrayTile(Array[Byte](5, 6, 7, 8), 2, 2))
    println("\n\n\n")
    println("THESE ARE THE ORIGINAL BYTEARRAYTILES")
    arr.foreach(println)
    println("\n\n\n")
    sc.parallelize(arr)
  }
}
