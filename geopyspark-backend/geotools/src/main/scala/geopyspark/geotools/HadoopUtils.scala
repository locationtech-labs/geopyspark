package geopyspark.geotools

import geotrellis.spark.io.hadoop._

import org.apache.spark._
import org.apache.hadoop.fs.Path

import java.net.URI


object HadoopUtils {
  def listFiles(sc: SparkContext, uris: Array[URI], extensions: Seq[String]): Array[String] =
    uris.flatMap { uri => listFiles(sc, uri, extensions) }

  def listFiles(sc: SparkContext, uri: URI, extensions: Seq[String]): Array[String] = {
    val path: Path = new Path(uri)
    val conf = sc.hadoopConfiguration.withInputDirectory(path, extensions)

    HdfsUtils
      .listFiles(path, conf)
      .map { _.toString }
      .filter { path => extensions.exists { e => path.endsWith(e) } }
      .toArray
  }
}
