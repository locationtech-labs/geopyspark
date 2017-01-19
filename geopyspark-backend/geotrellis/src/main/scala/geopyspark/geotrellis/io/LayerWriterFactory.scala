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

  def write(
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], schema: String,
    metadata: TileLayerMetadataWrapper[SpatialKey]
  ): Unit
}

class HadoopLayerWriterWrapper(uri: String, sc: SparkContext)
    extends LayerWriterWrapper {

  val attributeStore = HadoopAttributeStore(uri)(sc)
  val layerWriter = HadoopLayerWriter(uri)(sc)

  /**
    * Write the Java RDD of serialized objects (which will be
    * converted into a normal GeoTrellis RDD via the schema) into a
    * layer with the given name and zoom level.  The metadata written
    * into the layer are copied from the layer with id
    * LayerId(metaName, metaZoom).
    *
    * @param  name      The name to use for the layer being written
    * @param  zoom      The zoom level of the layer being written
    * @param  jrdd      The PySpark RDD to be written
    * @param  schema    A schema which will be used to convert jrdd
    * @param  metaName  The name of the layer from which the TileLayerMetadata should be copied
    * @param  metaZoom  The zoom level of the layer from which the TileLayerMetadata should be copied
    */
  def write(
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], schema: String,
    metaName: String, metaZoom: Int
  ): Unit = {
    val metaId = LayerId(metaName, metaZoom)
    val metadata = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](metaName, metaZoom)
    this.write(name, zoom, jrdd, schema, metadata)
  }

  /**
    * Write the Java RDD of serialized objects (which will be
    * converted into a normal GeoTrellis RDD via the schema) into a
    * layer with the given name and zoom level.
    *
    * @param  name      The name to use for the layer being written
    * @param  zoom      The zoom level of the layer being written
    * @param  jrdd      The PySpark RDD to be written
    * @param  schema    A schema which will be used to convert jrdd
    * @param  metadata  The metadata associated with the layer
    */
  def write(
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], schema: String,
    metadata: TileLayerMetadataWrapper[SpatialKey]
  ): Unit = {
    this.write(name, zoom, jrdd, schema, metadata.md)
  }

  /**
    * Write the Java RDD of serialized objects (which will be
    * converted into a normal GeoTrellis RDD via the schema) into a
    * layer with the given name and zoom level.
    *
    * @param  name      The name to use for the layer being written
    * @param  zoom      The zoom level of the layer being written
    * @param  jrdd      The PySpark RDD to be written
    * @param  schema    A schema which will be used to convert jrdd
    * @param  metadata  The metadata associated with the layer
    */
  private def write(
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], schema: String,
    metadata: TileLayerMetadata[SpatialKey]
  ): Unit = {
    val id = LayerId(name, zoom)
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
