package geopyspark.geotools.shapefile

import geopyspark.geotools._
import geotrellis.spark.io.hadoop._

import org.geotools.data.simple
import org.geotools.data.shapefile._
import org.opengis.feature.simple._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.hadoop.fs.Path

import java.io.File
import java.net.{URI, URL}

import scala.collection.mutable


object HadoopShapefileRDD {
  def createSimpleFeaturesRDD(
    sc: SparkContext,
    uris: Array[URI],
    extensions: Seq[String],
    numPartitions: Int
  ): RDD[SimpleFeature] =
    createSimpleFeaturesRDD(sc, HadoopUtils.listFiles(sc, uris, extensions), numPartitions)

  def createSimpleFeaturesRDD(
    sc: SparkContext,
    paths: Array[String],
    numPartitions: Int
  ): RDD[SimpleFeature] = {
    val urls = sc.parallelize(paths, numPartitions).map { new URL(_) }

    urls.flatMap { url =>
      val ds = new ShapefileDataStore(url)
      val ftItr = ds.getFeatureSource.getFeatures.features

      try {
        val simpleFeatures = mutable.ListBuffer[SimpleFeature]()
        while(ftItr.hasNext) simpleFeatures += ftItr.next()
        simpleFeatures.toList
      } finally {
        ftItr.close
        ds.dispose
      }
    }
  }
}
