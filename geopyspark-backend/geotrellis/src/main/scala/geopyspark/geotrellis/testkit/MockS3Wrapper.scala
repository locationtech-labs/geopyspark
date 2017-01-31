package geopyspark.geotrellis.testkit

import geotrellis.spark.io.s3._
import geotrellis.spark.io.s3.testkit._


object MockS3ClientWrapper {
  def mockClient = new MockS3Client()
}
