package geopyspark.geotrellis.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.s3._
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
  * Wrapper for the CassandraLayerReader class.
  */
class CassandraLayerWriterWrapper(
  as: CassandraAttributeStore,
  ks: String,
  table: String
) extends LayerWriterWrapper {

  val attributeStore = as
  val layerWriter = CassandraLayerWriter(as, ks, table)
}

/**
  * Wrapper for the FileLayerReader class.
  */
class FileLayerWriterWrapper(as: FileAttributeStore)
    extends LayerWriterWrapper {

  val attributeStore = as
  val layerWriter = FileLayerWriter(as)
}

/**
  * Wrapper for the S3LayerReader class.
  */
class S3LayerWriterWrapper(as: S3AttributeStore)
    extends LayerWriterWrapper {

  val attributeStore = as
  val layerWriter = S3LayerWriter(as)
}

/**
  * Wrapper for the HadoopLayerReader class.
  */
class HadoopLayerWriterWrapper(as: HadoopAttributeStore)
    extends LayerWriterWrapper {

  val attributeStore = as
  val layerWriter = HadoopLayerWriter(as.rootPath, as)
}

/**
  * Interface for requesting layer writer wrappers.  This object is
  * easily accessible from PySpark.
  */
object LayerWriterFactory {

  def buildHadoop(uri: String, sc: SparkContext) = {
    val as = HadoopAttributeStore(uri)(sc)
    new HadoopLayerWriterWrapper(as)
  }

  def buildHadoop(hasw: HadoopAttributeStoreWrapper) =
    new HadoopLayerWriterWrapper(hasw.attributeStore)

  def buildS3(bucket: String, root: String) = {
    val as = S3AttributeStore(bucket, root)
    new S3LayerWriterWrapper(as)
  }

  def buildS3(s3asw: S3AttributeStoreWrapper) =
    new S3LayerWriterWrapper(s3asw.attributeStore)

  def buildFile(path: String, sc: SparkContext) = {
    val as = FileAttributeStore(path)
    new FileLayerWriterWrapper(as)
  }

  def buildFile(fasw: FileAttributeStoreWrapper) =
    new FileLayerWriterWrapper(fasw.attributeStore)

  def buildCassandra(casw: CassandraAttributeStoreWrapper) =
    new CassandraLayerWriterWrapper(
      casw.attributeStore,
      casw.keySpace,
      casw.table
    )
}
