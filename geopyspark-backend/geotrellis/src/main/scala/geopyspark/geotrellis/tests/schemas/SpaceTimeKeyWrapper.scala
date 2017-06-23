package geopyspark.geotrellis.tests.schemas

import geopyspark.geotrellis._
import protos.keyMessages._
import geopyspark.geotrellis.testkit._
import geotrellis.spark._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD

object SpaceTimeKeyWrapper extends Wrapper2[SpaceTimeKey, ProtoSpaceTimeKey] {
  def testOut(sc: SparkContext): JavaRDD[Array[Byte]] =
    PythonTranslator.toPython[SpaceTimeKey, ProtoSpaceTimeKey](testRdd(sc))
  def testIn(rdd: RDD[Array[Byte]]) =
    PythonTranslator.fromPython[SpaceTimeKey, ProtoSpaceTimeKey](rdd, ProtoSpaceTimeKey.parseFrom)

  def testRdd(sc: SparkContext): RDD[SpaceTimeKey] = {
    val arr = Array(
      SpaceTimeKey(7, 3, 5.toLong),
      SpaceTimeKey(9, 4, 10.toLong),
      SpaceTimeKey(11, 5, 15.toLong))
    sc.parallelize(arr)
  }
}
