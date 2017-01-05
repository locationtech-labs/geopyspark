package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.spark.util.KryoWrapper

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.avro._

import scala.reflect.ClassTag

abstract class Wrapper[T: AvroRecordCodec: ClassTag] {
  def testOut(sc: SparkContext) =
    PythonTranslator.toPython(testRdd(sc))

  def testIn(rdd: RDD[Array[Byte]], schema: String) =
    PythonTranslator.fromPython[T](rdd, Some(schema)).foreach(println)

  def testRdd(sc: SparkContext): RDD[T]

  def encodeRdd(rdd: RDD[T]): RDD[Array[Byte]] = {
    rdd.map { key => AvroEncoder.toBinary(key, deflate = false)
    }
  }

  def encodeRddText(rdd: RDD[T]): RDD[String] = {
      rdd.map { key => AvroEncoder.toBinary(key, deflate = false).mkString("")
      }
    }

  def keySchema: String = {
    implicitly[AvroRecordCodec[T]].schema.toString
  }
}
