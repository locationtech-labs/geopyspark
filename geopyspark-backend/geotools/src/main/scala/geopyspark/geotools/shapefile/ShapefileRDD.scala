package geopyspark.geotools.shapefile

import geopyspark.geotools._
import geopyspark.util._

import protos.simpleFeatureMessages._

import geotrellis.vector._
import geotrellis.spark.io.s3._
import geotrellis.spark.io.s3.testkit._
import geotrellis.geotools._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.api.java.JavaRDD

import org.opengis.feature.simple._

import java.net.URI

import scala.collection.JavaConverters._


object ShapefileRDD {
  def get(
    sc: SparkContext,
    paths: java.util.ArrayList[String],
    extensions: java.util.ArrayList[String],
    numPartitions: Int,
    s3Client: String
  ): JavaRDD[Array[Byte]] = {
    val uris: Array[URI] = paths.asScala.map { path => new URI(path) }.toArray

    val client =
      s3Client match {
        case null => S3Client.DEFAULT
        case s: String =>
          s match {
            case "default" => S3Client.DEFAULT
            case "mock" => new MockS3Client()
            case _ => throw new Exception(s"Unkown S3Client specified, ${s}")
          }
      }

    val simpleFeaturesRDD: RDD[SimpleFeature] =
      uris.head.getScheme match {
        case "s3" =>
          S3ShapefileRDD.createSimpleFeaturesRDD(sc, uris, extensions.asScala, client, numPartitions)
        case _ =>
          HadoopShapefileRDD.createSimpleFeaturesRDD(sc, uris, extensions.asScala, numPartitions)
      }

    val featuresRDD: RDD[Feature[Geometry, Map[String, AnyRef]]] =
      simpleFeaturesRDD.map { SimpleFeatureToFeature(_) }

    PythonTranslator.toPython[Feature[Geometry, Map[String, AnyRef]], ProtoSimpleFeature](featuresRDD)
  }
}
