package geopyspark.geotrellis

import geopyspark.geotrellis._

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.spark.io.hadoop._

import org.apache.spark._
import org.apache.spark.rdd.RDD




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

object GeoTiffRDD {
  import Constants._

  def countIt(rdd: RDD[(ProjectedExtent, MultibandTile)]): Long =
    rdd.count()

  def countItAgain(rdd: RDD[(ProjectedExtent, Tile)]): Long = {
    require(rdd.id == 100, "Wrong ID, lol")
    rdd.count()
  }

  def options(opts: java.util.HashMap[String, Any]): Int = {
    println(opts.toString)
    3
  }

  def getRDD(
    sc: SparkContext,
    keyType: String,
    uri: String
  ): RasterRDD[ProjectedExtent] = {
    // TODO: turns options into Optional
    // TODO: Q: lets make ... cut_tiles
      new ProjectedRasterRDD(HadoopGeoTiffRDD.spatialMultiband(uri)(sc))
  }
}
