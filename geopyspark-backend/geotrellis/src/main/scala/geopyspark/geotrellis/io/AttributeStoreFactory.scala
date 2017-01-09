package geopyspark.geotrellis.io

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._

import org.apache.spark._


abstract class AttributeStoreWrapper {
  def header(name: String, zoom: Int): Array[String]
  def cellType(name: String, zoom: Int): String
}

class HadoopAttributeStoreWrapper(uri: String, sc: SparkContext)
    extends AttributeStoreWrapper {

  val attributeStore = HadoopAttributeStore(uri)(sc)

  def header(name: String, zoom: Int): Array[String] = {
    val h = attributeStore.readHeader[HadoopLayerHeader](LayerId(name, zoom))
    Array[String](h.keyClass, h.valueClass, h.path.toString)
  }

  def cellType(name: String, zoom: Int): String = {
    val id = LayerId(name, zoom)
    val h = attributeStore.readHeader[HadoopLayerHeader](id)

    h.keyClass match {
      case "geotrellis.spark.SpatialKey" => {
        attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](id).cellType.toString
      }
      case "geotrellis.spark.SpaceTimeKey" => {
        attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](id).cellType.toString
      }
      case _ => throw new Exception
    }
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
