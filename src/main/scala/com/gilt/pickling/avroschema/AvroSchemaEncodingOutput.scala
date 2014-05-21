package com.gilt.pickling.avroschema

import scala.pickling.Output
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream

class AvroSchemaEncodingOutput extends Output[Array[Byte]] {
  val stream = new ByteArrayOutputStream()
  val encoder = EncoderFactory.get.directBinaryEncoder(stream, null)

  override def put(obj: Array[Byte]): this.type = {
    stream.write(obj)
    this
  }

  override def result(): Array[Byte] = {
    encoder.flush()
    stream.toByteArray
  }
}
