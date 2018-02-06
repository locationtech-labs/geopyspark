package geopyspark.geotools.protobufs

import geopyspark.util._
import geotrellis.vector._
import geotrellis.vector.io.wkb.WKB

import protos.simpleFeatureMessages._

import scala.collection.JavaConverters._

import com.google.protobuf.ByteString


trait SimpleFeatureProtoBuf {
  implicit def featureProtoBufCodec = new ProtoBufCodec[Feature[Geometry, Map[String, AnyRef]], ProtoSimpleFeature] {
    def encode(feature: Feature[Geometry, Map[String, AnyRef]]): ProtoSimpleFeature = {
      val geom = feature.geom
      val data = feature.data

      val geomBytes: ByteString = ByteString.copyFrom(WKB.write(geom))
      val dataMap: Map[String, String] = data.mapValues { case v => if (v == null) "" else v.toString }

      ProtoSimpleFeature(geom = geomBytes, metadata = dataMap)
    }

    // TODO: Implement a decoding method
    def decode(message: ProtoSimpleFeature): Feature[Geometry, Map[String, String]] =
      ???
  }
}
