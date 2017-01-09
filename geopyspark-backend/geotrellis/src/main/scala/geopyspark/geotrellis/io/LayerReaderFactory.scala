package geopyspark.geotrellis.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import geopyspark.geotrellis.PythonTranslator


abstract class LayerReaderWrapper {
  def query(name: String, zoom: Int): (JavaRDD[Array[Byte]], String)
}

class HadoopLayerReaderWrapper(uri: String, sc: SparkContext)
    extends LayerReaderWrapper {

  val attributeStore = HadoopAttributeStore(uri)(sc)
  val layerReader = HadoopLayerReader(uri)(sc)

  def query(name: String, zoom: Int): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    // val results = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)
    val subset = Extent(-87.1875, 34.43409789359469, -78.15673828125, 39.87601941962116)
    val results = layerReader
      .query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)
      .where(Intersects(subset))
      .result

    PythonTranslator.toPython(results)
  }
}

object LayerReaderFactory {
  def build(backend: String, uri: String, sc: SparkContext): LayerReaderWrapper = {
    backend match {
      case "hdfs" => new HadoopLayerReaderWrapper(uri, sc)
      case _ => throw new Exception
    }
  }
}
