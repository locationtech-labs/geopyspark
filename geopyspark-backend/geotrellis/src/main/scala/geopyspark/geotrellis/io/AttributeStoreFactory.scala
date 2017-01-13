package geopyspark.geotrellis.io

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.s3._

import org.apache.spark._


/**
  * Base wrapper class for various types of attribute store wrappers.
  */
abstract class AttributeStoreWrapper {
  def attributeStore: AttributeStore

  def header(name: String, zoom: Int): Array[String]

  def metadataSpatial(name: String, zoom: Int): TileLayerMetadataWrapper[SpatialKey] = {
    val id = LayerId(name, zoom)
    val md = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](id)
    new TileLayerMetadataWrapper(md)
  }

  def metadataSpaceTime(name: String, zoom: Int): TileLayerMetadataWrapper[SpaceTimeKey] = {
    val id = LayerId(name, zoom)
    val md = attributeStore.readMetadata[TileLayerMetadata[SpaceTimeKey]](id)
    new TileLayerMetadataWrapper(md)
  }
}

/**
  * S3AttributeStore wrapper.
  *
  * @param  bucket  The name of the S3 bucket
  * @param  root    The location of the layer within the bucket
  */
class S3AttributeStoreWrapper(bucket: String, root: String)
    extends AttributeStoreWrapper {

  val attributeStore = S3AttributeStore(bucket, root)

  def header(name: String, zoom: Int): Array[String] = {
    val h = attributeStore.readHeader[S3LayerHeader](LayerId(name, zoom))
    Array[String](h.keyClass, h.valueClass, h.bucket, h.key)
  }
}

/**
  * HadoopAttributeStore wrapper.
  *
  * @param  uri  The URI where the catalog is located
  * @param  sc   The SparkContext
  */
class HadoopAttributeStoreWrapper(uri: String, sc: SparkContext)
    extends AttributeStoreWrapper {

  val sparkContext = sc
  val attributeStore = HadoopAttributeStore(uri)(sparkContext)

  def header(name: String, zoom: Int): Array[String] = {
    val h = attributeStore.readHeader[HadoopLayerHeader](LayerId(name, zoom))
    Array[String](h.keyClass, h.valueClass, h.path.toString)
  }
}

/**
  * Interface for requesting attribute store wrappers.  This object is
  * easily accessible from PySpark.
  */
object AttributeStoreFactory {

  def buildHadoop(uri: String, sc: SparkContext): AttributeStoreWrapper =
    new HadoopAttributeStoreWrapper(uri, sc)

  def buildS3(bucket: String, root: String): AttributeStoreWrapper =
    new S3AttributeStoreWrapper(bucket, root)
}
