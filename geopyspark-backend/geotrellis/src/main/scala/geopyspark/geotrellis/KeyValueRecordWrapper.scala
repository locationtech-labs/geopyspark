package geopyspark.geotrellis

import geotrellis.raster._
import geotrellis.vector.Extent
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.util.KryoWrapper
import geotrellis.spark.io.avro.codecs._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaRDD
import org.apache.avro._

object KeyValueRecordWrapper {

  def codec: KeyValueRecordCodec[ByteArrayTile, Extent] = KeyValueRecordCodec[ByteArrayTile, Extent]

  def testOut(sc: SparkContext) =
    toPython(testRdd(sc))

  def testIn(rdd: RDD[Array[Byte]], schema: String) =
    fromPython(rdd, Some(schema)).foreach(println)

  def encodeRdd(rdd: RDD[Vector[(ByteArrayTile, Extent)]]): RDD[Array[Byte]] = {
    rdd.map { key => AvroEncoder.toBinary(key, deflate = false)(codec)
    }
  }

  def encodeRddText(rdd: RDD[Vector[(ByteArrayTile, Extent)]]): RDD[String] = {
      rdd.map { key => AvroEncoder.toBinary(key, deflate = false)(codec).mkString("")
      }
    }

  def keySchema: String = {
    codec.schema.toString
  }

  def testRdd(sc: SparkContext): RDD[Vector[(ByteArrayTile, Extent)]] = {
    val vector = Vector(
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 2, 3) -> Extent(0, 0, 1, 1)),
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 3, 2) -> Extent(1, 2, 3, 4)),
      (ByteArrayTile(Array[Byte](0, 1, 2, 3, 4, 5), 1, 6) -> Extent(5, 6, 7, 8)))

    val arr = Array(vector, vector)

    sc.parallelize(arr)
  }

  def toPython(rdd: RDD[Vector[(ByteArrayTile, Extent)]]): (JavaRDD[Array[Byte]], String) = {
    val jrdd =
      rdd
        .map { v =>
          AvroEncoder.toBinary(v, deflate = false)(codec)
        }
        .toJavaRDD
    (jrdd, keySchema)
  }

  def fromPython(rdd: RDD[Array[Byte]], schemaJson: Option[String] = None): RDD[Vector[(ByteArrayTile, Extent)]] = {
    val schema = schemaJson.map { json => (new Schema.Parser).parse(json) }
    val _recordCodec = codec
    val kwWriterSchema = KryoWrapper(schema)

    rdd.map { bytes =>
      AvroEncoder.fromBinary(kwWriterSchema.value.getOrElse(_recordCodec.schema), bytes, false)(_recordCodec)
    }
  }
}
