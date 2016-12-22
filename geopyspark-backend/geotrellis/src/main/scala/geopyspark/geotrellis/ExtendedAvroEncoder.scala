
package geopyspark.geotrellis
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs._

import java.io.ByteArrayInputStream

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

  def fromBinary[T: AvroRecordCodec](bytes: Array[Byte]): T = {
    val format = implicitly[AvroRecordCodec[T]]
    fromBinary[T](format.schema, bytes)
  }

  def fromBinary[T: AvroRecordCodec](writerSchema: Schema, bytes: Array[Byte]): T = {
    val format = implicitly[AvroRecordCodec[T]]
    val schema = format.schema

    val reader = new GenericDatumReader[GenericRecord](writerSchema, schema)
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    try {
      val rec = reader.read(null.asInstanceOf[GenericRecord], decoder)
      format.decode(rec)
    } catch {
      case e: AvroTypeException =>
        throw new AvroTypeException(e.getMessage + ". " +
          "This can be caused by using a type parameter which doesn't match the object being deserialized.")
    }
  }
}
