package geopyspark.geotrellis.io

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._

import org.apache.spark._


/**
  * Base wrapper class for various types of attribute stores.
  */
abstract class AttributeStoreWrapper {
  def header(name: String, zoom: Int): Array[String]
  def metadata(name: String, zoom: Int): TileLayerMetadataWrapper[SpatialKey]
}

/**
  * HadoopAttributeStore wrapper.
  *
  * @param  uri  The URI where the catalog is located
  * @param  sc   The SparkContext
  */
class HadoopAttributeStoreWrapper(uri: String, sc: SparkContext)
    extends AttributeStoreWrapper {

  val attributeStore = HadoopAttributeStore(uri)(sc)

  def header(name: String, zoom: Int): Array[String] = {
    val h = attributeStore.readHeader[HadoopLayerHeader](LayerId(name, zoom))
    Array[String](h.keyClass, h.valueClass, h.path.toString)
  }

  def metadata(name: String, zoom: Int): TileLayerMetadataWrapper[SpatialKey] = {
    val id = LayerId(name, zoom)
    val md = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](id)
    new TileLayerMetadataWrapper(md)
  }
}

/**
  * Interface for requesting attribute store wrappers.  This object is
  * easily accessible from PySpark.
  */
object AttributeStoreFactory {
  def build(backend: String, uri: String, sc: SparkContext): AttributeStoreWrapper = {
    backend match {
      case "hdfs" => new HadoopAttributeStoreWrapper(uri, sc)
      case _ => throw new Exception
    }
  }
}
