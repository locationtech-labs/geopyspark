package geopyspark.geotrellis.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import geopyspark.geotrellis.PythonTranslator


abstract class LayerReaderWrapper {
  def read(name: String, zoom: Int): (JavaRDD[Array[Byte]], String)
  def query(name: String, zoom: Int, queryObjectStr: String): (JavaRDD[Array[Byte]], String)
}

class HadoopLayerReaderWrapper(uri: String, sc: SparkContext)
    extends LayerReaderWrapper {

  val attributeStore = HadoopAttributeStore(uri)(sc)
  val layerReader = HadoopLayerReader(uri)(sc)

  /**
    * Read the layer with the given name and zoom level.
    *
    * @param  name  The name of the layer to read
    * @param  zoom  The zoom level of the requested layer
    */
  def read(name: String, zoom: Int): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val results = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)

    PythonTranslator.toPython(results)
  }

  /**
    * Read a subset of the layer with the given name and zoom level.
    * The subset is controlled by a Geometry communicated via WKT.  If
    * the Geometry is a Point, then a "Contains" query is performed,
    * otherwise if a Polygon or MultiPolygon is used, an "Intersects"
    * query is done.
    *
    * @param  name            The name of the layer to read
    * @param  zoom            The zoom level of the requested layer
    * @param  queryObjectStr  The query object in WKT
    */
  def query(name: String, zoom: Int, queryObjectStr: String): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val layer = layerReader.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)
    val query = WKT.read(queryObjectStr) match {
      case point: Point => layer.where(Contains(point))
      case polygon: Polygon => layer.where(Intersects(polygon))
      case multi: MultiPolygon => layer.where(Intersects(multi))
      case _ => throw new Exception
    }

    PythonTranslator.toPython(query.result)
  }
}

object LayerReaderFactory {

  /**
    * Build the reader object.
    *
    * @param  backend  The name of backend that should be used (e.g. "hdfs")
    * @param  uri      The URI of the catalog (given as a string)
    * @param  sc       The SparkContext
    */
  def build(backend: String, uri: String, sc: SparkContext): LayerReaderWrapper = {
    backend match {
      case "hdfs" => new HadoopLayerReaderWrapper(uri, sc)
      case _ => throw new Exception
    }
  }
}
