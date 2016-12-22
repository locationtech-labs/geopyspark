package geopyspark.geotrellis

import geotrellis.vector.Extent
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.util.KryoWrapper

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.avro._

import scala.reflect.ClassTag

object ExtentWrapper {
  def testOut(sc: SparkContext) =
    PythonTranslator.toPython(testRdd(sc))

  def testIn(rdd: RDD[Array[Byte]], schema: String) =
    PythonTranslator.fromPython[Extent](rdd, Some(schema)).foreach(println)

  def testRdd(sc: SparkContext): RDD[Extent] = {
    val arr = Array(
      Extent(0, 0, 1, 1),
      Extent(1, 2, 3, 4),
      Extent(5, 6, 7, 8))
    println("\n\n\n")
    println("THESE ARE THE ORIGINAL EXTENTS")
    arr.foreach(println)
    println("\n\n\n")
    sc.parallelize(arr)
  }

  def encodeRdd(rdd: RDD[Extent]): RDD[Array[Byte]] = {
    rdd.map { key => ExtendedAvroEncoder.toBinary(key, deflate = false)
    }
  }

  def encodeRddText(rdd: RDD[Extent]): RDD[String] = {
      rdd.map { key => ExtendedAvroEncoder.toBinary(key, deflate = false).mkString("")
      }
    }

  def keySchema: String = {
    implicitly[AvroRecordCodec[Extent]].schema.toString
  }

  def makeRasterExtent(rdd: RDD[Array[Byte]]): Unit = {
    val newRdd: RDD[Extent] = rdd.map(x => ExtendedAvroEncoder.fromBinary[Extent](x))
    val rasterExtents: RDD[RasterExtent] = newRdd.map(x => RasterExtent(x, 500, 500))
    val collection = rasterExtents.collect()
    println("\n\n\n")
    println("THIS IS THE RESULT OF TURNING THE EXTENTS INTO RASTER EXTENTS")
    collection.foreach(println)
    println("\n\n\n")
  }
}

object PythonTranslator {
  def toPython[T: AvroRecordCodec](rdd: RDD[T]): (JavaRDD[Array[Byte]], String) = {
    val jrdd =
      rdd
        .map { v =>
          ExtendedAvroEncoder.toBinary(v, deflate = false)
        }
        .toJavaRDD
    (jrdd, implicitly[AvroRecordCodec[T]].schema.toString)
  }

  def fromPython[T: AvroRecordCodec: ClassTag](rdd: RDD[Array[Byte]], schemaJson: Option[String] = None): RDD[T] = {
    val schema = schemaJson.map { json => (new Schema.Parser).parse(json) }
    val _recordCodec = implicitly[AvroRecordCodec[T]]
    val kwWriterSchema = KryoWrapper(schema)

    rdd.map { bytes =>
      AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(_recordCodec.schema), bytes)(_recordCodec)
    }
  }
}
