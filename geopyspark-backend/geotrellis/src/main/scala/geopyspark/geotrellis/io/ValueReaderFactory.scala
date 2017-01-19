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

  def reader(k: String, v: String, id: LayerId) = {
    (k, v) match {
      case ("spatial", "singleband") =>
        (valueReader.reader[SpatialKey, Tile](id).read)_
      case ("spacetime", "singleband") =>
        (valueReader.reader[SpaceTimeKey, Tile](id).read)_
      case ("spatial", "multiband") =>
        (valueReader.reader[SpatialKey, MultibandTile](id).read)_
      case ("spacetime", "multiband") =>
        (valueReader.reader[SpaceTimeKey, MultibandTile](id).read)_
    }
  }

}

/**
  * Wrapper for the AccumuloValueReader class.
  */
class AccumuloValueReaderWrapper(
  k: String, v: String,
  id: LayerId,
  in: AccumuloInstance,
  as: AccumuloAttributeStore
) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new AccumuloValueReader(in, as)
  val read = reader(k, v, id)
}

/**
  * Wrapper for the HBaseValueReader class.
  */
class HBaseValueReaderWrapper(
  k: String, v: String,
  id: LayerId,
  in: HBaseInstance,
  as: HBaseAttributeStore
) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new HBaseValueReader(in, as)
  val read = reader(k, v, id)
}

/**
  * Wrapper for the CassandraValueReader class.
  */
class CassandraValueReaderWrapper(
  k: String, v: String,
  id: LayerId,
  in: CassandraInstance,
  as: CassandraAttributeStore
) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new CassandraValueReader(in, as)
  val read = reader(k, v, id)
}

/**
  * Wrapper for the FileValueReader class.
  */
class FileValueReaderWrapper(
  k: String, v: String,
  id: LayerId,
  path: String,
  as: FileAttributeStore
) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new FileValueReader(as, path)
  val read = reader(k, v, id)
}

/**
  * Wrapper for the S3ValueReader class.
  */
class S3ValueReaderWrapper(
  k: String, v: String,
  id: LayerId,
  as: S3AttributeStore
) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new S3ValueReader(as)
  val read = reader(k, v, id)
}

/**
  * Wrapper for the HadoopValueReader class.
  */
class HadoopValueReaderWrapper(
  k: String, v: String,
  id: LayerId,
  as: HadoopAttributeStore
) extends ValueReaderWrapper {
  val attributeStore = as
  val valueReader = new HadoopValueReader(as, as.hadoopConfiguration)
  val read = reader(k, v, id)
}

/**
  * Interface for requesting vlaue reader wrappers.  This object is
  * easily accessible from PySpark.
  */
object ValueReaderFactory {

  def buildHadoop(
    k: String, v: String,
    layerName: String, zoom: Int,
    hasw: HadoopAttributeStoreWrapper) = {
    val id = LayerId(layerName, zoom)
    new HadoopValueReaderWrapper(k, v, id, hasw.attributeStore)
  }

  def buildS3(
    k: String, v: String,
    layerName: String, zoom: Int,
    s3asw: S3AttributeStoreWrapper) = {
    val id = LayerId(layerName, zoom)
    new S3ValueReaderWrapper(k, v, id, s3asw.attributeStore)
  }

  def buildFile(
    k: String, v: String,
    layerName: String, zoom: Int,
    fasw: FileAttributeStoreWrapper) = {
    val attributeStore = fasw.attributeStore
    val path = attributeStore.catalogPath
    val id = LayerId(layerName, zoom)
    new FileValueReaderWrapper(k, v, id, path, attributeStore)
  }

  def buildCassandra(
    k: String, v: String,
    layerName: String, zoom: Int,
    casw: CassandraAttributeStoreWrapper) = {
    val attributeStore = casw.attributeStore
    val instance = attributeStore.instance
    val id = LayerId(layerName, zoom)
    new CassandraValueReaderWrapper(k, v, id, instance, attributeStore)
  }

  def buildHBase(
    k: String, v: String,
    layerName: String, zoom: Int,
    hbasw: HBaseAttributeStoreWrapper) = {
    val attributeStore = hbasw.attributeStore
    val instance = attributeStore.instance
    val id = LayerId(layerName, zoom)
    new HBaseValueReaderWrapper(k, v, id, instance, attributeStore)
  }

  def buildAccumulo(
    k: String, v: String,
    layerName: String, zoom: Int,
    aasw: AccumuloAttributeStoreWrapper) = {
    val id = LayerId(layerName, zoom)
    new AccumuloValueReaderWrapper(k, v, id, aasw.instance, aasw.attributeStore)
  }
}
