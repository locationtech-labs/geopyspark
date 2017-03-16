package geopyspark.geotrellis

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.avro._


object SchemaProducer {
  def getSchema(keyType: String): String =
    keyType match {
      case "ProjectedExtent" =>
        implicitly[AvroRecordCodec[(ProjectedExtent, MultibandTile)]].schema.toString
      case "TemporalProjectedExtent" =>
        implicitly[AvroRecordCodec[(TemporalProjectedExtent, MultibandTile)]].schema.toString

      case "SpatialKey" =>
        implicitly[AvroRecordCodec[(SpatialKey, MultibandTile)]].schema.toString
      case "SpaceTimeKey" =>
        implicitly[AvroRecordCodec[(SpaceTimeKey, MultibandTile)]].schema.toString
    }
}
