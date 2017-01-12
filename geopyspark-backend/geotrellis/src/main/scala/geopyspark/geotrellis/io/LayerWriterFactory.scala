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


/**
  * Base wrapper class for all backends that provide a
  * LayerWriter[LayerId].
  */
abstract class LayerWriterWrapper {

  def attributeStore: AttributeStore
  def layerWriter: LayerWriter[LayerId]

  /**
    * Write the Java RDD of serialized objects (which will be
    * converted into a normal GeoTrellis RDD via the schema) into a
    * space-time layer with the given name and zoom level.
    *
    * @param  name      The name to use for the layer being written
    * @param  zoom      The zoom level of the layer being written
    * @param  jrdd      The PySpark RDD to be written
    * @param  schema    A schema which will be used to convert jrdd
    * @param  metadata  The metadata associated with the layer
    */
  def write(
    k: String, v: String, unit: String,
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], schema: String,
    metadata: TileLayerMetadataWrapper[Any]
  ): Unit = {
    val id = LayerId(name, zoom)
    val indexMethod = unit match {
      case "millis" => ZCurveKeyIndexMethod.byMilliseconds(1)
      case "seconds" => ZCurveKeyIndexMethod.bySecond
      case "minutes" => ZCurveKeyIndexMethod.byMinute
      case "hour" => ZCurveKeyIndexMethod.byHour
      case "days" => ZCurveKeyIndexMethod.byDay
      case "months" => ZCurveKeyIndexMethod.byMonth
      case "years" => ZCurveKeyIndexMethod.byYear
      case _ => if (k == "spacetime") throw new Exception; else null
    }

    (k, v) match {
      case ("spatial", "singleband") => {
        val rawRdd = PythonTranslator.fromPython[(SpatialKey, Tile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpatialKey]])
        layerWriter.write(id, rdd, ZCurveKeyIndexMethod)
      }
      case ("spatial", "multiband") => {
        val rawRdd = PythonTranslator.fromPython[(SpatialKey, MultibandTile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpatialKey]])
        layerWriter.write(id, rdd, ZCurveKeyIndexMethod)
      }
      case ("spacetime", "singleband") => {
        val rawRdd = PythonTranslator.fromPython[(SpaceTimeKey, Tile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpaceTimeKey]])
        layerWriter.write(id, rdd, indexMethod)
      }
      case ("spacetime", "multiband") => {
        val rawRdd = PythonTranslator.fromPython[(SpaceTimeKey, MultibandTile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpaceTimeKey]])
        layerWriter.write(id, rdd, indexMethod)
      }
    }
  }
}

/**
  * Wrapper for the HadoopLayerReader class.
  */
class HadoopLayerWriterWrapper(uri: String, sc: SparkContext)
    extends LayerWriterWrapper {

  val attributeStore = HadoopAttributeStore(uri)(sc)
  val layerWriter = HadoopLayerWriter(uri)(sc)
}

/**
  * Interface for requesting layer writer wrappers.  This object is
  * easily accessible from PySpark.
  */
object LayerWriterFactory {
  def build(backend: String, uri: String, sc: SparkContext): LayerWriterWrapper = {
    backend match {
      case "hdfs" => new HadoopLayerWriterWrapper(uri, sc)
      case _ => throw new Exception
    }
  }
}
