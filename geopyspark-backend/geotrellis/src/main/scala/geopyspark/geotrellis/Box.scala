package geopyspark.geotrellis

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.avro._

object Box {
  def testRdd(sc: SparkContext): RDD[SpatialKey] = {
    val arr = Array(
      SpatialKey(7,3))
    sc.parallelize(arr)
  }

  def encodeRdd(rdd: RDD[SpatialKey]): RDD[Array[Byte]] = {
    rdd.map { key => ExtendedAvroEncoder.toBinary(key, deflate = false)
    }
  }

  def encodeRddText(rdd: RDD[SpatialKey]): RDD[String] = {
      rdd.map { key => ExtendedAvroEncoder.toBinary(key, deflate = false).mkString("")
      }
    }

  def keySchema: String = {
    implicitly[AvroRecordCodec[SpatialKey]].schema.toString
  }
}
