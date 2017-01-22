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
abstract class ValueReaderWrapper() {
  def attributeStore: AttributeStore
  def valueReader: ValueReader[LayerId]

  def readSpatialSingleband(
    layerName: String, zoom: Int,
    col: Int, row: Int
  ): (Array[Byte], String) = {
    val id = LayerId(layerName, zoom)
    val key = SpatialKey(col, row)
    PythonTranslator.toPython(valueReader.reader[SpatialKey, Tile](id).read(key))
  }

  def readSpatialMultiband(
    layerName: String, zoom: Int,
    col: Int, row: Int
  ): (Array[Byte], String) = {
    val id = LayerId(layerName, zoom)
    val key = SpatialKey(col, row)
    PythonTranslator.toPython(valueReader.reader[SpatialKey, MultibandTile](id).read(key))
  }

  def readSpacetimeSingleband(
    layerName: String, zoom: Int,
    col: Int, row: Int, zdt: String
  ): (Array[Byte], String) = {
    val id = LayerId(layerName, zoom)
    val key = SpaceTimeKey(col, row, ZonedDateTime.parse(zdt))
    PythonTranslator.toPython(valueReader.reader[SpaceTimeKey, Tile](id).read(key))
  }

  def readSpacetimeMultiband(
    layerName: String, zoom: Int,
    col: Int, row: Int, zdt: String
  ): (Array[Byte], String) = {
    val id = LayerId(layerName, zoom)
    val key = SpaceTimeKey(col, row, ZonedDateTime.parse(zdt))
    PythonTranslator.toPython(valueReader.reader[SpaceTimeKey, MultibandTile](id).read(key))
  }

}

/**
  * Wrapper for the AccumuloValueReader class.
  */
class AccumuloValueReaderWrapper(in: AccumuloInstance, as: AccumuloAttributeStore) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new AccumuloValueReader(in, as)
}

/**
  * Wrapper for the HBaseValueReader class.
  */
class HBaseValueReaderWrapper(in: HBaseInstance, as: HBaseAttributeStore) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new HBaseValueReader(in, as)
}

/**
  * Wrapper for the CassandraValueReader class.
  */
class CassandraValueReaderWrapper(in: CassandraInstance, as: CassandraAttributeStore) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new CassandraValueReader(in, as)
}

/**
  * Wrapper for the FileValueReader class.
  */
class FileValueReaderWrapper(path: String, as: FileAttributeStore) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new FileValueReader(as, path)
}

/**
  * Wrapper for the S3ValueReader class.
  */
class S3ValueReaderWrapper(as: S3AttributeStore) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new S3ValueReader(as)
}

/**
  * Wrapper for the HadoopValueReader class.
  */
class HadoopValueReaderWrapper(as: HadoopAttributeStore) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new HadoopValueReader(as, as.hadoopConfiguration)
}

/**
  * Interface for requesting vlaue reader wrappers.  This object is
  * easily accessible from PySpark.
  */
object ValueReaderFactory {

  def buildHadoop(hasw: HadoopAttributeStoreWrapper) =
    new HadoopValueReaderWrapper(hasw.attributeStore)

  def buildS3(s3asw: S3AttributeStoreWrapper) =
    new S3ValueReaderWrapper(s3asw.attributeStore)

  def buildFile(fasw: FileAttributeStoreWrapper) = {
    val attributeStore = fasw.attributeStore
    val path = attributeStore.catalogPath
    new FileValueReaderWrapper(path, attributeStore)
  }

  def buildCassandra(casw: CassandraAttributeStoreWrapper) = {
    val attributeStore = casw.attributeStore
    val instance = attributeStore.instance
    new CassandraValueReaderWrapper(instance, attributeStore)
  }

  def buildHBase(hbasw: HBaseAttributeStoreWrapper) = {
    val attributeStore = hbasw.attributeStore
    val instance = attributeStore.instance
    new HBaseValueReaderWrapper(instance, attributeStore)
  }

  def buildAccumulo(aasw: AccumuloAttributeStoreWrapper) =
    new AccumuloValueReaderWrapper(aasw.instance, aasw.attributeStore)
}
