package geopyspark.geotools

import geotrellis.spark.io.s3._

import org.apache.spark._

import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}

import java.net.URI

import scala.collection.JavaConverters._
import scala.collection.mutable


object S3Utils {
  def listKeys(uris: Array[URI], extensions: Seq[String], s3Client: S3Client): Array[String] =
    uris.flatMap { uri => listKeys(uri, extensions, s3Client) }

  def listKeys(uri: URI, extensions: Seq[String], s3Client: S3Client): Array[String] =
    listKeys(uri.getHost, uri.getPath.tail, extensions, s3Client)

  def listKeys(
    s3bucket: String,
    s3prefix: String,
    extensions: Seq[String],
    s3Client: S3Client
  ): Array[String] = {
    val objectRequest = (new ListObjectsRequest)
      .withBucketName(s3bucket)
      .withPrefix(s3prefix)

    listKeys(objectRequest, s3Client)
      .filter { path => extensions.exists { e => path.endsWith(e) } }
      .collect { case key =>
        s"https://s3.amazonaws.com/${s3bucket}/${key}"
      }.toArray
  }

  // Copied from GeoTrellis codebase
  def listKeys(listObjectsRequest: ListObjectsRequest, s3Client: S3Client): Array[String] = {
    var listing: ObjectListing = null
    val result = mutable.ListBuffer[String]()
    do {
      listing = s3Client.listObjects(listObjectsRequest)
      // avoid including "directories" in the input split, can cause 403 errors on GET
      result ++= listing.getObjectSummaries.asScala.map(_.getKey).filterNot(_ endsWith "/")
      listObjectsRequest.setMarker(listing.getNextMarker)
    } while (listing.isTruncated)

    result.toArray
  }
}
