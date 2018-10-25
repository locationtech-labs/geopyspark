package geopyspark.geotrellis.io

import geopyspark.geotrellis._
import protos.tileMessages._

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.cog._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
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

import geopyspark.util.PythonTranslator



/**
  * General interface for reading.
  */
class ValueReaderWrapper(uri: String) {
  val attributeStore = AttributeStore(uri)

  lazy val cogReader: COGValueReader[LayerId] = COGValueReader(uri)
  lazy val avroReader: ValueReader[LayerId] = ValueReader(uri)

  def getValueClass(id: LayerId): String =
    attributeStore.readHeader[LayerHeader](id).valueClass

  def readTile(
    layerName: String,
    zoom: Int,
    col: Int,
    row: Int,
    zdt: String
  ): Array[Byte] = {
    val id = LayerId(layerName, zoom)

    val header = produceHeader(attributeStore, id)

    val valueReader: Either[COGValueReader[LayerId], ValueReader[LayerId]] =
      header.layerType match {
        case COGLayerType => Left(cogReader)
        case _ => Right(avroReader)
      }

    try {
      (header.keyClass, header.valueClass) match {
        case ("geotrellis.spark.SpatialKey", "geotrellis.raster.Tile") => {
          val spatialKey = SpatialKey(col, row)
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpatialKey, Tile](id).read(spatialKey)
            case Right(avroReader) => avroReader.reader[SpatialKey, Tile](id).read(spatialKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](MultibandTile(result))
        }
        case ("geotrellis.spark.SpatialKey", "geotrellis.raster.MultibandTile") => {
          val spatialKey = SpatialKey(col, row)
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpatialKey, MultibandTile](id).read(spatialKey)
            case Right(avroReader) => avroReader.reader[SpatialKey, MultibandTile](id).read(spatialKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](result)
        }
        case ("geotrellis.spark.SpaceTimeKey", "geotrellis.raster.Tile") => {
          val spaceKey = SpaceTimeKey(col, row, ZonedDateTime.parse(zdt))
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpaceTimeKey, Tile](id).read(spaceKey)
            case Right(avroReader) => avroReader.reader[SpaceTimeKey, Tile](id).read(spaceKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](MultibandTile(result))
        }
        case ("geotrellis.spark.SpaceTimeKey", "geotrellis.raster.MultibandTile") => {
          val spaceKey = SpaceTimeKey(col, row, ZonedDateTime.parse(zdt))
          val result = valueReader match {
            case Left(cogReader) => cogReader.reader[SpaceTimeKey, MultibandTile](id).read(spaceKey)
            case Right(avroReader) => avroReader.reader[SpaceTimeKey, MultibandTile](id).read(spaceKey)
          }
          PythonTranslator.toPython[MultibandTile, ProtoMultibandTile](result)
        }
      }
    } catch {
      case e: ValueNotFoundError => return null
    }
  }
}
