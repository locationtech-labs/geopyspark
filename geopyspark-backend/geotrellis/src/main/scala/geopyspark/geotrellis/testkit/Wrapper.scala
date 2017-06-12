package geopyspark.geotrellis.testkit

import geopyspark.geotrellis._
import geotrellis.spark._
import geotrellis.spark.io.avro._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag

import com.trueaccord.scalapb.GeneratedMessage


abstract class Wrapper2[T, M <: GeneratedMessage] {
  def testOut(sc: SparkContext): JavaRDD[Array[Byte]]
  def testIn(rdd: RDD[Array[Byte]])
  def testRdd(sc: SparkContext): RDD[T]
}
