package geopyspark.geotrellis.io

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.hbase._
import geotrellis.spark.io.s3._
import geotrellis.vector._
import geotrellis.vector.io.wkt.WKT

import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import java.time.ZonedDateTime
import java.util.ArrayList
import scala.collection.JavaConverters._
import scala.collection.mutable

import geopyspark.geotrellis.PythonTranslator


/**
  * General interface for reading.
  */
abstract class LayerReaderWrapper {
  def readSpatialSingleband(
    name: String,
    zoom: Int
  ): (JavaRDD[Array[Byte]], String)

  def readSpatialMultiband(
    name: String,
    zoom: Int
  ): (JavaRDD[Array[Byte]], String)

  def readSpaceTimeSingleband(
    name: String,
    zoom: Int):
  (JavaRDD[Array[Byte]], String)

  def readSpaceTimeMultiband(
    name: String,
    zoom: Int
  ): (JavaRDD[Array[Byte]], String)

  def querySpatialSingleband(
    name: String,
    zoom: Int,
    queryGeometryStr: String
  ): (JavaRDD[Array[Byte]], String)

  def querySpatialMultiband(
    name: String,
    zoom: Int,
    queryGeometryStr: String
  ): (JavaRDD[Array[Byte]], String)

  def querySpaceTimeSingleband(
    name: String,
    zoom: Int,
    queryGeometryStr: String,
    queryIntervalStrs: ArrayList[String]
  ): (JavaRDD[Array[Byte]], String)

  def querySpaceTimeMultiband(
    name: String,
    zoom: Int,
    queryGeometryStr: String,
    queryIntervalStrs: ArrayList[String]
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

  def readSpatialSingleband(name: String, zoom: Int): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val results = layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](id)
    PythonTranslator.toPython(results)
  }

  def readSpatialMultiband(name: String, zoom: Int): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val results = layerReader.read[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](id)
    PythonTranslator.toPython(results)
  }

  def readSpaceTimeSingleband(name: String, zoom: Int): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val results = layerReader.read[SpaceTimeKey, Tile, TileLayerMetadata[SpaceTimeKey]](id)
    PythonTranslator.toPython(results)
  }

  def readSpaceTimeMultiband(name: String, zoom: Int): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val results = layerReader.read[SpaceTimeKey, MultibandTile, TileLayerMetadata[SpaceTimeKey]](id)
    PythonTranslator.toPython(results)
  }

  private def getSpatialQuery(queryGeometryStr: String) =
    queryGeometryStr match {
      case "" => None
      case str: String => Some(WKT.read(queryGeometryStr))
      case _ => throw new Exception
    }

  private def getTemporalQuery(queryIntervalStrs: ArrayList[String]) = {
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
  }

  def querySpatialSingleband(
    name: String,
    zoom: Int,
    queryGeometryStr: String
  ): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val spatialQuery = getSpatialQuery(queryGeometryStr)

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

  def querySpatialMultiband(
    name: String,
    zoom: Int,
    queryGeometryStr: String
  ): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val spatialQuery = getSpatialQuery(queryGeometryStr)

    val layer = layerReader.query[SpatialKey, MultibandTile, TileLayerMetadata[SpatialKey]](id)
    val query = spatialQuery match {
      case Some(polygon: Polygon) => layer.where(Intersects(polygon))
      case Some(multi: MultiPolygon) => layer.where(Intersects(multi))
      case None => layer
      case _ => throw new Exception("Unsupported Geometry")
    }

    PythonTranslator.toPython(query.result)
  }

  def querySpaceTimeSingleband(
    name: String,
    zoom: Int,
    queryGeometryStr: String,
    queryIntervalStrs: ArrayList[String]
  ): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val spatialQuery = getSpatialQuery(queryGeometryStr)
    val temporalQuery = getTemporalQuery(queryIntervalStrs)

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

  def querySpaceTimeMultiband(
    name: String,
    zoom: Int,
    queryGeometryStr: String,
    queryIntervalStrs: ArrayList[String]
  ): (JavaRDD[Array[Byte]], String) = {
    val id = LayerId(name, zoom)
    val spatialQuery = getSpatialQuery(queryGeometryStr)
    val temporalQuery = getTemporalQuery(queryIntervalStrs)

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


/**
  * Wrapper for the AccumuloLayerReader class.
  */
class AccumuloLayerReaderWrapper(
  in: AccumuloInstance,
  as: AccumuloAttributeStore,
  sc: SparkContext
) extends FilteringLayerReaderWrapper {

  val attributeStore = as
  val layerReader = AccumuloLayerReader(in)(sc)
}

/**
  * Wrapper for the HBaseLayerReader class.
  */
class HBaseLayerReaderWrapper(as: HBaseAttributeStore, sc: SparkContext)
    extends FilteringLayerReaderWrapper {

  val attributeStore = as
  val layerReader = HBaseLayerReader(as)(sc)
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
  val layerReader = HadoopLayerReader(as)(sc)
}

/**
  * Interface for requesting layer reader wrappers.  This object is
  * easily accessible from PySpark.
  */
object LayerReaderFactory {

  def buildHadoop(hasw: HadoopAttributeStoreWrapper) =
    new HadoopLayerReaderWrapper(hasw.attributeStore, hasw.sparkContext)

  def buildS3(s3asw: S3AttributeStoreWrapper, sc: SparkContext) =
    new S3LayerReaderWrapper(s3asw.attributeStore, sc)

  def buildFile(fasw: FileAttributeStoreWrapper, sc: SparkContext) =
    new FileLayerReaderWrapper(fasw.attributeStore, sc)

  def buildCassandra(casw: CassandraAttributeStoreWrapper, sc: SparkContext) =
    new CassandraLayerReaderWrapper(casw.attributeStore, sc)

  def buildHBase(hbasw: HBaseAttributeStoreWrapper, sc: SparkContext) =
    new HBaseLayerReaderWrapper(hbasw.attributeStore, sc)

  def buildAccumulo(aasw: AccumuloAttributeStoreWrapper, sc: SparkContext) =
    new AccumuloLayerReaderWrapper(aasw.instance, aasw.attributeStore, sc)
}
