package geopyspark.geotools.shapefile

import geopyspark.geotools._
import geotrellis.spark.io.s3._

import org.apache.spark._
import org.apache.spark.rdd._

import org.geotools.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.feature._
import org.geotools.feature.simple._

import org.opengis.feature.simple._
import org.opengis.feature.`type`.Name

import com.amazonaws.auth._
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable


class IteratorWrapper[I, T](iter: I)(hasNext: I => Boolean, next: I => T, close: I => Unit) extends Iterator[T] {
  def hasNext = {
    val has = hasNext(iter)
    if (! has) close(iter)
    has
  }
  def next = next(iter)
}


object S3ShapefileRDD {
  def createSimpleFeaturesRDD(
    sc: SparkContext,
    uris: Array[URI],
    extensions: Seq[String],
    s3Client: S3Client,
    numPartitions: Int
  ): RDD[SimpleFeature] =
    createSimpleFeaturesRDD(sc, S3Utils.listKeys(uris, extensions, s3Client), numPartitions)

  def createSimpleFeaturesRDD(
    sc: SparkContext,
    urlArray: Array[String],
    numPartitions: Int
  ): RDD[SimpleFeature] = {
    val urlRdd: RDD[String] = sc.parallelize(urlArray, numPartitions)
    urlRdd.mapPartitions { urls =>
      urls.flatMap { url =>
        val datastoreParams = Map("url" -> url).asJava
        val shpDS = DataStoreFinder.getDataStore(datastoreParams)
        require(shpDS != null, "Could not build ShapefileDataStore")

        shpDS.getNames.asScala.flatMap { name: Name =>
          val features =
            shpDS
            .getFeatureSource(name)
            .getFeatures
            .features
          new IteratorWrapper(features)(_.hasNext, _.next, _.close)
        }
      }
    }
  }
}
