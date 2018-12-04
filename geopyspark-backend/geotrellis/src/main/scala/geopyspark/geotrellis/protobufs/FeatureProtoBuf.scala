package geopyspark.geotrellis.protobufs

import geopyspark.util._
import geotrellis.vector._
import geotrellis.vector.io.wkb.WKB
import geotrellis.raster.rasterize._

import protos.featureMessages._

//import java.time.ZonedDateTime

import com.google.protobuf.ByteString


trait FeatureProtoBuf {
  implicit def featureCellValueProtoBufCodec = new ProtoBufCodec[Feature[Geometry, CellValue], ProtoFeatureCellValue] {
    def encode(feature: Feature[Geometry, CellValue]): ProtoFeatureCellValue = {
      val geom = feature.geom
      val data = feature.data

      val cellValue = ProtoCellValue(value = data.value, zindex = data.zindex)
      val geomBytes: ByteString = ByteString.copyFrom(WKB.write(geom))

      ProtoFeatureCellValue(geom = geomBytes, cellValue = Some(cellValue))
    }

    def decode(message: ProtoFeatureCellValue): Feature[Geometry, CellValue] = {
      val geomBytes: Array[Byte] = message.geom.toByteArray
      val cellValue = message.cellValue.get

      Feature(WKB.read(geomBytes), CellValue(cellValue.value, cellValue.zindex))
    }
  }
}

