package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.util.KryoWrapper

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.avro._

object ByteArrayTileWrapper extends Wrapper[ByteArrayTile] {

  override def testIn(rdd: RDD[Array[Byte]], schema: String) = {
    val result = PythonTranslator.fromPython[ByteArrayTile](rdd, Some(schema))
    val collection = result.collect()
    println("\n\n\nTHESE ARE THE NEW ARRAYS")
    collection.map(x => println(x.array.mkString(" ")))
  }

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
