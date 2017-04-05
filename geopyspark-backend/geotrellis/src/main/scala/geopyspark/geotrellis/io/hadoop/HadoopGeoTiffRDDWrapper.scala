package geopyspark.geotrellis.io.hadoop

import geopyspark.geotrellis._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.avro._

import scala.collection.JavaConverters._
import java.util.Map

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD

import org.apache.hadoop.fs.Path


object HadoopGeoTiffRDDOptions {
  def default = HadoopGeoTiffRDD.Options.DEFAULT

  def setValues(javaMap: java.util.Map[String, Any]): HadoopGeoTiffRDD.Options = {
    val stringValues = Array("timeTag", "timeFormat", "crs")

    val (stringMap, intMap) = GeoTrellisUtils.convertToScalaMap(javaMap, stringValues)

    val crs: Option[CRS] =
      if (stringMap.contains("crs"))
        Some(CRS.fromName(stringMap("crs")))
      else
        None

    HadoopGeoTiffRDD.Options(
      crs = crs,
      timeTag = stringMap.getOrElse("timeTag", default.timeTag),
      timeFormat = stringMap.getOrElse("timeFormat", default.timeFormat),
      maxTileSize = intMap.get("maxTileSize"),
      numPartitions = intMap.get("numPartitions"),
      chunkSize = intMap.get("chunkSize"))
  }
}


object HadoopGeoTiffRDDWrapper {
  def getRDD(
    keyType: String,
    path: String,
    sc: SparkContext
  ): (JavaRDD[Array[Byte]], String) =
    keyType match {
      case "ProjectedExtent" =>
        PythonTranslator.toPython(HadoopGeoTiffRDD.spatialMultiband(path)(sc))
      case "TemporalProjectedExtent" =>
        PythonTranslator.toPython(HadoopGeoTiffRDD.temporalMultiband(path)(sc))
    }

  def getRDD(
    keyType: String,
    path: String,
    options: java.util.Map[String, Any],
    sc: SparkContext): (JavaRDD[Array[Byte]], String) = {
    val hadoopOptions = HadoopGeoTiffRDDOptions.setValues(options)
    keyType match {
      case "ProjectedExtent" =>
        PythonTranslator.toPython(HadoopGeoTiffRDD.spatialMultiband(path, hadoopOptions)(sc))
      case "TemporalProjectedExtent" =>
        PythonTranslator.toPython(HadoopGeoTiffRDD.temporalMultiband(path, hadoopOptions)(sc))
    }
  }
}
