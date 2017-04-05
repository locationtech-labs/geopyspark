package geopyspark.geotrellis.spark.mask

import geopyspark.geotrellis._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.mask.Mask
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT

import spray.json._

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.reflect.ClassTag


object MaskWrapper {

  private def mask[K: SpatialComponent: ClassTag: AvroRecordCodec: JsonFormat](
    javaRdd: JavaRDD[Array[Byte]],
    schema: String,
    metadata: TileLayerMetadata[K],
    geometries: Seq[MultiPolygon]
  ): (JavaRDD[Array[Byte]], String) = {
    val _rdd = PythonTranslator.fromPython[(K, MultibandTile)](javaRdd, Some(schema))
    val rdd = ContextRDD(_rdd, metadata)
    val options = Mask.Options.DEFAULT

    PythonTranslator.toPython(Mask(rdd, geometries, options))
  }

  def mask(
    keyType: String,
    javaRdd: JavaRDD[Array[Byte]],
    schema: String,
    metadataStr: String,
    wkts: java.util.ArrayList[String]
  ): (JavaRDD[Array[Byte]], String) = {
    val geometries: Seq[MultiPolygon] = wkts
      .asScala.map({ wkt => WKT.read(wkt) })
      .flatMap({
        case p: Polygon => Some(MultiPolygon(p))
        case m: MultiPolygon => Some(m)
        case _ => None
      })

    keyType match {
      case "SpatialKey" => {
        val metadataAST = metadataStr.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        mask[SpatialKey](javaRdd, schema, metadata, geometries)
      }
      case "SpaceTimeKey" => {
        val metadataAST = metadataStr.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        mask[SpaceTimeKey](javaRdd, schema, metadata, geometries)
      }
    }
  }

}
