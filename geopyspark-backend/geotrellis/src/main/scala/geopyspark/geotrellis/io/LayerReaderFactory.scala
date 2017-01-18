package geopyspark.geotrellis.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT

import collection.JavaConverters._
import collection.mutable
import java.time.ZonedDateTime
import java.util.ArrayList
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import geopyspark.geotrellis.PythonTranslator


/**
  * General interface for reading.
  */
abstract class LayerReaderWrapper {
  def read(
    k: String, v: String,
    name: String, zoom: Int
  ): (JavaRDD[Array[Byte]], String)

  def query(
    k: String, v: String,
    name: String, zoom: Int,
    queryGeometryStr: String, queryIntervalStr: ArrayList[String]
  ): (JavaRDD[Array[Byte]], String)
}

/**
  * Base wrapper class for all backends that provide a
  * FilteringLayerReader[LayerId].
  */
abstract class FilteringLayerReaderWrapper()
    extends LayerReaderWrapper {

  def attributeStore: AttributeStore
  def layerReader: FilteringLayerReader[LayerId]

  /**
    * Read the layer with the given name and zoom level.
    *
    * @param  name  The name of the layer to read
    * @param  zoom  The zoom level of the requested layer
    */
  def read(k: String, v: String, name: String, zoom: Int): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)

    (k, v) match {
      case ("spatial", "singleband") => {
        val results = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)
        PythonTranslator.toPython(results)
      }
      case ("spacetime", "singleband") => {
        val results = layerReader.read[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](id)
        PythonTranslator.toPython(results)
      }
      case ("spatial", "multiband") => {
        val results = layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](id)
        PythonTranslator.toPython(results)
      }
      case ("spacetime", "multiband") => {
        val results = layerReader.read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
        PythonTranslator.toPython(results)
      }
    }
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
  def query(
    k: String, v: String,
    name: String, zoom: Int,
    queryGeometryStr: String, queryIntervalStrs: ArrayList[String]
  ): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val spatialQuery = queryGeometryStr match {
      case "" => None
      case str: String => Some(WKT.read(queryGeometryStr))
      case _ => throw new Exception
    }
    val temporalRanges = queryIntervalStrs
      .asScala
      .grouped(2)
      .map({ list =>
        list match {
          case mutable.Buffer(a, b) =>
            Between(ZonedDateTime.parse(a), ZonedDateTime.parse(b))
          case mutable.Buffer(a) => {
            val zdt = ZonedDateTime.parse(a)
            val zdt1 = zdt.minusNanos(16)
            val zdt2 = zdt.plusNanos(17)
            Between(zdt1, zdt2)
          }
        }})
      .toList
    val temporalQuery =
      if (temporalRanges.length >= 2) {
        val h1 = temporalRanges.head
        val h2 = temporalRanges.tail.head
        Some(temporalRanges.drop(2).foldLeft(h1 or h2)({ (cs, c) => cs or c }))
      }
      else if (temporalRanges.length == 1) {
        val h1 = temporalRanges.head
        Some(h1 or h1)
      }
      else None

    (k, v) match {
      case ("spatial", "singleband") => {
        val layer = layerReader.query[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)
        val query = spatialQuery match {
          case Some(point: Point) => layer.where(Contains(point))
          case Some(polygon: Polygon) => layer.where(Intersects(polygon))
          case Some(multi: MultiPolygon) => layer.where(Intersects(multi))
          case None => layer
          case _ => throw new Exception(s"Unsupported Geometry $spatialQuery")
        }
        PythonTranslator.toPython(query.result)
      }
      case ("spacetime", "singleband") => {
        val layer = layerReader.query[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](id)
        val query1 = spatialQuery match {
          case Some(polygon: Polygon) => layer.where(Intersects(polygon))
          case Some(multi: MultiPolygon) => layer.where(Intersects(multi))
          case None => layer
          case _ => throw new Exception("Unsupported Geometry")
        }
        val query2 = temporalQuery match {
          case Some(q) => query1.where(q)
          case None => query1
        }
        PythonTranslator.toPython(query2.result)
      }
      case ("spatial", "multiband") => {
        val layer = layerReader.query[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](id)
        val query = spatialQuery match {
          case Some(polygon: Polygon) => layer.where(Intersects(polygon))
          case Some(multi: MultiPolygon) => layer.where(Intersects(multi))
          case None => layer
          case _ => throw new Exception("Unsupported Geometry")
        }
        PythonTranslator.toPython(query.result)
      }
      case ("spacetime", "multiband") => {
        val layer = layerReader.query[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
        val query1 = spatialQuery match {
          case Some(multi: MultiPolygon) => layer.where(Intersects(multi))
          case None => layer
          case _ => throw new Exception("Unsupported Geometry")
        }
        val query2 = temporalQuery match {
          case Some(q) => query1.where(q)
          case None => query1
        }
        PythonTranslator.toPython(query2.result)
      }
    }
  }
}

/**
  * Wrapper for the CassandraLayerReader class.
  */
class CassandraLayerReaderWrapper(as: CassandraAttributeStore, sc: SparkContext)
    extends FilteringLayerReaderWrapper {

  val attributeStore = as
  val layerReader = CassandraLayerReader(as)(sc)
}

/**
  * Wrapper for the FileLayerReader class.
  */
class FileLayerReaderWrapper(as: FileAttributeStore, sc: SparkContext)
    extends FilteringLayerReaderWrapper {

  val attributeStore = as
  val layerReader = FileLayerReader(as)(sc)
}

/**
  * Wrapper for the S3LayerReader class.
  */
class S3LayerReaderWrapper(as: S3AttributeStore, sc: SparkContext)
    extends FilteringLayerReaderWrapper {

  val attributeStore = as
  val layerReader = S3LayerReader(as)(sc)
}

/**
  * Wrapper for the HadoopLayerReader class.
  */
class HadoopLayerReaderWrapper(as: HadoopAttributeStore, sc: SparkContext)
    extends FilteringLayerReaderWrapper {

  val attributeStore = as
  val layerReader = HadoopLayerReader(attributeStore)(sc)
}

/**
  * Interface for requesting layer reader wrappers.  This object is
  * easily accessible from PySpark.
  */
object LayerReaderFactory {

  def buildHadoop(uri: String, sc: SparkContext) = {
    val as = HadoopAttributeStore(uri)(sc)
    new HadoopLayerReaderWrapper(as, sc)
  }

  def buildHadoop(hasw: HadoopAttributeStoreWrapper) =
    new HadoopLayerReaderWrapper(hasw.attributeStore, hasw.sparkContext)

  def buildS3(bucket: String, root: String, sc: SparkContext) = {
    val as = S3AttributeStore(bucket, root)
    new S3LayerReaderWrapper(as, sc)
  }

  def buildS3(s3asw: S3AttributeStoreWrapper, sc: SparkContext) =
    new S3LayerReaderWrapper(s3asw.attributeStore, sc)

  def buildFile(path: String, sc: SparkContext) = {
    val as = FileAttributeStore(path)
    new FileLayerReaderWrapper(as, sc)
  }

  def buildFile(fasw: FileAttributeStoreWrapper, sc: SparkContext) =
    new FileLayerReaderWrapper(fasw.attributeStore, sc)

  def buildCassandra(casw: CassandraAttributeStoreWrapper, sc: SparkContext) =
    new CassandraLayerReaderWrapper(casw.attributeStore, sc)
}
