package geopyspark.geotrellis

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._

import java.io.ByteArrayInputStream
import java.util.zip.{InflaterInputStream, DeflaterOutputStream, Deflater}

import org.apache.avro._
import org.apache.avro.io._
import org.apache.avro.generic._
import org.apache.commons.io.IOUtils
import org.apache.commons.io.output.ByteArrayOutputStream

object ExtendedAvroEncoder {
  def toBinary[T: AvroRecordCodec](thing: T, deflate: Boolean = true): Array[Byte] = {
    val format = implicitly[AvroRecordCodec[T]]
    val schema: Schema = format.schema

    val writer = new GenericDatumWriter[GenericRecord](schema)
    val jos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(jos, null)
    writer.write(format.encode(thing), encoder)
    encoder.flush()
    if (deflate)
      AvroEncoder.compress(jos.toByteArray)
    else
      jos.toByteArray
  }

  def toBinary[T: AvroRecordCodec](thing: T): Array[Byte] = {
    val format = implicitly[AvroRecordCodec[T]]
    val schema: Schema = format.schema

    val writer = new GenericDatumWriter[GenericRecord](schema)
    val jos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(jos, null)
    writer.write(format.encode(thing), encoder)
    encoder.flush()
    AvroEncoder.compress(jos.toByteArray)
  }
}
