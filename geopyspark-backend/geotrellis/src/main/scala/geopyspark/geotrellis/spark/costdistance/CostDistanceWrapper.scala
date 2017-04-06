package geopyspark.geotrellis.spark.costdistance

import geopyspark.geotrellis._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT
import geotrellis.spark.costdistance.IterativeCostDistance

import spray.json._

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import scala.reflect.ClassTag


object CostDistanceWrapper {

  implicit def convertion(k: SpaceTimeKey): SpatialKey =
    k.spatialKey

  private def costdistance[K: (? => SpatialKey): ClassTag: AvroRecordCodec: JsonFormat](
    javaRdd: JavaRDD[Array[Byte]],
    schema: String,
    metadata: TileLayerMetadata[K],
    geometries: Seq[Geometry],
    maxDistance: Double,
    _sc: SparkContext
  ): (JavaRDD[Array[Byte]], String) = {
    implicit val sc = _sc
    val _rdd = PythonTranslator.fromPython[(K, MultibandTile)](javaRdd, Some(schema))
    val rdd = ContextRDD(
      _rdd.map({ case (k, v) => (k, v.band(0)) }),
      metadata
    )

    PythonTranslator.toPython(IterativeCostDistance(rdd, geometries, maxDistance))
  }

  def costdistance(
    keyType: String,
    javaRdd: JavaRDD[Array[Byte]],
    schema: String,
    metadataStr: String,
    wkts: java.util.ArrayList[String],
    maxDistance: java.lang.Double,
    sc: SparkContext
  ): (JavaRDD[Array[Byte]], String) = {
    val geometries = wkts.asScala.map({ wkt => WKT.read(wkt) })

    keyType match {
      case "SpatialKey" => {
        val metadataAST = metadataStr.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpatialKey]]
        costdistance[SpatialKey](javaRdd, schema, metadata, geometries, maxDistance, sc)
      }
      case "SpaceTimeKey" => {
        val metadataAST = metadataStr.parseJson
        val metadata = metadataAST.convertTo[TileLayerMetadata[SpaceTimeKey]]
        costdistance[SpaceTimeKey](javaRdd, schema, metadata, geometries, maxDistance, sc)
      }
    }
  }

}
