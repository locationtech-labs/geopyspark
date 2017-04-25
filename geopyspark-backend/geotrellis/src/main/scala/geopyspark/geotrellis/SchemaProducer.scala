package geopyspark.geotrellis

import geotrellis.vector._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.avro._


object SchemaProducer {
  import Constants._

  def getSchema(keyType: String): String =
    keyType match {
      case PROJECTEDEXTENT =>
        implicitly[AvroRecordCodec[(ProjectedExtent, MultibandTile)]].schema.toString
      case TEMPORALPROJECTEDEXTENT =>
        implicitly[AvroRecordCodec[(TemporalProjectedExtent, MultibandTile)]].schema.toString

      case SPATIALKEY =>
        implicitly[AvroRecordCodec[(SpatialKey, MultibandTile)]].schema.toString
      case SPACETIMEKEY =>
        implicitly[AvroRecordCodec[(SpaceTimeKey, MultibandTile)]].schema.toString
    }
}
