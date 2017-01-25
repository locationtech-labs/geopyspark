package geopyspark.geotrellis.io.s3

import geopyspark.geotrellis._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.s3._
//import geotrellis.spark.io.avro._

import scala.collection.JavaConverters._
import java.util.Map

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD


object S3GeoTiffRDDOptions {
  def default = S3GeoTiffRDD.Options.DEFAULT

  def setValues(javaMap: java.util.Map[String, Any]): S3GeoTiffRDD.Options = {
    //TODO: Find a better way of creating Options from python

    val stringValues = List("timeTag", "timeFormat")
    val scalaMap = javaMap.asScala

    val intMap =
      scalaMap.filterKeys(x => !(stringValues.contains(x)))
        .mapValues(x => x.asInstanceOf[Int])

    val stringMap =
      scalaMap.filterKeys(x => stringValues.contains(x))
        .mapValues(x => x.asInstanceOf[String])

    val crs: Option[CRS] =
      if (intMap.contains("crs"))
        Some(CRS.fromEpsgCode(intMap("crs")))
      else
        None

    S3GeoTiffRDD.Options(
      crs = crs,
      timeTag = stringMap.getOrElse("timeTag", default.timeTag),
      timeFormat = stringMap.getOrElse("timeFormat", default.timeFormat),
      maxTileSize = intMap.get("maxTileSize"),
      numPartitions = intMap.get("numPartitions"),
      chunkSize = intMap.get("chunkSize"))
  }
}


object S3GeoTiffRDDWrapper {
  def spatial(
    bucket: String,
    prefix: String,
    sc: SparkContext): (JavaRDD[Array[Byte]], String) =
      PythonTranslator.toPython(S3GeoTiffRDD.spatial(bucket, prefix)(sc))

  def spatial(bucket: String,
    prefix: String,
    options: java.util.Map[String, Any],
    sc: SparkContext): (JavaRDD[Array[Byte]], String) = {
      val s3Options = S3GeoTiffRDDOptions.setValues(options)

      PythonTranslator.toPython(S3GeoTiffRDD.spatial(bucket, prefix, s3Options)(sc))
  }

  def spatialMultiband(
    bucket: String,
    prefix: String,
    sc: SparkContext): (JavaRDD[Array[Byte]], String) =
      PythonTranslator.toPython(S3GeoTiffRDD.spatialMultiband(bucket, prefix)(sc))

  def spatialMultiband(
    bucket: String,
    prefix: String,
    options: java.util.Map[String, Any],
    sc: SparkContext): (JavaRDD[Array[Byte]], String) = {
      val s3Options = S3GeoTiffRDDOptions.setValues(options)

      PythonTranslator.toPython(S3GeoTiffRDD.spatialMultiband(bucket, prefix, s3Options)(sc))
  }

  def temporal(
    bucket: String,
    prefix: String,
    sc: SparkContext): (JavaRDD[Array[Byte]], String) =
      PythonTranslator.toPython(S3GeoTiffRDD.temporal(bucket, prefix)(sc))

  def temporal(
    bucket: String,
    prefix: String,
    options: java.util.Map[String, Any],
    sc: SparkContext): (JavaRDD[Array[Byte]], String) = {
      val s3Options = S3GeoTiffRDDOptions.setValues(options)

      PythonTranslator.toPython(S3GeoTiffRDD.temporal(bucket, prefix, s3Options)(sc))
  }

  def temporalMultiband(
    bucket: String,
    prefix: String,
    sc: SparkContext): (JavaRDD[Array[Byte]], String) =
      PythonTranslator.toPython(S3GeoTiffRDD.temporalMultiband(bucket, prefix)(sc))

  def temporalMultiband(
    bucket: String,
    prefix: String,
    options: java.util.Map[String, Any],
    sc: SparkContext): (JavaRDD[Array[Byte]], String) = {
      val s3Options = S3GeoTiffRDDOptions.setValues(options)

      PythonTranslator.toPython(S3GeoTiffRDD.temporalMultiband(bucket, prefix, s3Options)(sc))
  }
}
