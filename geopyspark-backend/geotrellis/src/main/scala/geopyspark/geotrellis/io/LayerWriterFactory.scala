package geopyspark.geotrellis.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import geopyspark.geotrellis.PythonTranslator


abstract class LayerWriterWrapper {
  def write(
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], schema: String,
    metaname: String, metazoom: Int
  ): Unit
}

class HadoopLayerWriterWrapper(uri: String, sc: SparkContext)
    extends LayerWriterWrapper {

  val attributeStore = HadoopAttributeStore(uri)(sc)
  val layerWriter = HadoopLayerWriter(uri)(sc)

  def write(
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], schema: String,
    metaName: String, metaZoom: Int
  ): Unit = {
    val id = LayerId(name, zoom)
    val metaId = LayerId(metaName, metaZoom)
    // val metadata = attributeStore.readMetadata[SpatialKey](metaName, metaZoom)
    val layerReader = HadoopLayerReader(uri)(sc)
    val metadata: TileLayerMetadata[SpatialKey] = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](metaId).metadata
    val rawRdd = PythonTranslator.fromPython[(SpatialKey, Tile)](jrdd, Some(schema))
    val rdd = ContextRDD(rawRdd, metadata)

    layerWriter.write(id, rdd, ZCurveKeyIndexMethod)
  }
}

object LayerWriterFactory {
  def build(backend: String, uri: String, sc: SparkContext): LayerWriterWrapper = {
    backend match {
      case "hdfs" => new HadoopLayerWriterWrapper(uri, sc)
      case _ => throw new Exception
    }
  }
}
