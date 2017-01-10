package geopyspark.geotrellis.io

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._

import org.apache.spark._


abstract class AttributeStoreWrapper {
  def header(name: String, zoom: Int): Array[String]
  def metadata(name: String, zoom: Int): TileLayerMetadataWrapper[SpatialKey]
}

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

object AttributeStoreFactory {
  def build(backend: String, uri: String, sc: SparkContext): AttributeStoreWrapper = {
    backend match {
      case "hdfs" => new HadoopAttributeStoreWrapper(uri, sc)
      case _ => throw new Exception
    }
  }
}
