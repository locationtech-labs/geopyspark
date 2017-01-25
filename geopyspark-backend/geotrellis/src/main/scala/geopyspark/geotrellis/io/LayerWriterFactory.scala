package geopyspark.geotrellis.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hbase._
import geotrellis.spark.io.index._
import geotrellis.spark.io.index.hilbert._
import geotrellis.spark.io.s3._
import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime

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
    * @param  name           The name to use for the layer being written
    * @param  zoom           The zoom level of the layer being written
    * @param  jrdd           The PySpark RDD to be written
    * @param  metadata       The metadata associated with the layer
    * @param  schema         A schema which will be used to convert jrdd
    * @param  timeString     A string controlling temporal indexing
    * @param  indexStrategy  The index strategy to be used
    */
  def write(
    keyType: String, valueType: String,
    name: String, zoom: Int,
    jrdd: JavaRDD[Array[Byte]], metadata: TileLayerMetadataWrapper[Any], schema: String,
    timeString: String, indexStrategy: String
  ): Unit = {
    val id = LayerId(name, zoom)

    /* SpatialKey */
    if (keyType == "spatial") {
      val indexMethod: KeyIndexMethod[SpatialKey] = indexStrategy match {
        case "zorder" => ZCurveKeyIndexMethod
        case "hilbert" => HilbertKeyIndexMethod
        case "rowmajor" => RowMajorKeyIndexMethod
        case _ => throw new Exception
      }

      /* Tile */
      if (valueType == "singleband") {
        val rawRdd = PythonTranslator.fromPython[(SpatialKey, Tile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpatialKey]])
        layerWriter.write(id, rdd, indexMethod)
      }
      /* MultibandTile */
      else if (valueType == "multiband") {
        val rawRdd = PythonTranslator.fromPython[(SpatialKey, MultibandTile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpatialKey]])
        layerWriter.write(id, rdd, indexMethod)
      }
      else throw new Exception
    }
    /* SpaceTimeKey */
    else if (keyType == "spacetime") {
      val indexMethod: KeyIndexMethod[SpaceTimeKey] = (indexStrategy, timeString) match {
        case ("zorder", "millis") => ZCurveKeyIndexMethod.byMilliseconds(1)
        case ("zorder", "seconds") => ZCurveKeyIndexMethod.bySecond
        case ("zorder", "minutes") => ZCurveKeyIndexMethod.byMinute
        case ("zorder", "hour") => ZCurveKeyIndexMethod.byHour
        case ("zorder", "days") => ZCurveKeyIndexMethod.byDay
        case ("zorder", "months") => ZCurveKeyIndexMethod.byMonth
        case ("zorder", "years") => ZCurveKeyIndexMethod.byYear
        case ("hilbert", _) => {
          timeString.split(",") match {
            case Array(minDate, maxDate, resolution) =>
              HilbertKeyIndexMethod(ZonedDateTime.parse(minDate), ZonedDateTime.parse(maxDate), resolution.toInt)
            case Array(resolution) =>
              HilbertKeyIndexMethod(resolution.toInt)
            case _ => throw new Exception(s"Invalid timeString: $timeString")
          }
        }
        case _ => throw new Exception
      }

      /* Tile */
      if (valueType == "singleband") {
        val rawRdd = PythonTranslator.fromPython[(SpaceTimeKey, Tile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpaceTimeKey]])
        layerWriter.write(id, rdd, indexMethod)
      }
      /* MultibandTile */
      else if (valueType == "multiband") {
        val rawRdd = PythonTranslator.fromPython[(SpaceTimeKey, MultibandTile)](jrdd, Some(schema))
        val rdd = ContextRDD(rawRdd, metadata.get.asInstanceOf[TileLayerMetadata[SpaceTimeKey]])
        layerWriter.write(id, rdd, indexMethod)
      }
    }
  }

}

/**
  * Wrapper for the AccumuloLayerReader class.
  */
class AccumuloLayerWriterWrapper(
  in: AccumuloInstance,
  as: AccumuloAttributeStore,
  table: String
) extends LayerWriterWrapper {

  val attributeStore = as
  val layerWriter = AccumuloLayerWriter(in, as, table)
}

/**
  * Wrapper for the HBaseLayerReader class.
  */
class HBaseLayerWriterWrapper(
  as: HBaseAttributeStore,
  table: String
) extends LayerWriterWrapper {

  val attributeStore = as
  val layerWriter = HBaseLayerWriter(as, table)
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

  def buildHadoop(hasw: HadoopAttributeStoreWrapper) =
    new HadoopLayerWriterWrapper(hasw.attributeStore)

  def buildS3(s3asw: S3AttributeStoreWrapper) =
    new S3LayerWriterWrapper(s3asw.attributeStore)

  def buildFile(fasw: FileAttributeStoreWrapper) =
    new FileLayerWriterWrapper(fasw.attributeStore)

  def buildCassandra(casw: CassandraAttributeStoreWrapper) =
    new CassandraLayerWriterWrapper(
      casw.attributeStore,
      casw.keySpace,
      casw.table
    )

  def buildHBase(hbasw: HBaseAttributeStoreWrapper) =
    new HBaseLayerWriterWrapper(hbasw.attributeStore, hbasw.table)

  def buildAccumulo(aasw: AccumuloAttributeStoreWrapper) =
    new AccumuloLayerWriterWrapper(aasw.instance, aasw.attributeStore, aasw.table)
}
