package com.gilt.pickling

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import java.io.{ByteArrayOutputStream, File}
import org.apache.avro.generic.{GenericRecord, GenericDatumWriter, GenericData}
import org.apache.avro.io.{EncoderFactory, BinaryEncoder}

object TestUtils {

  def retrieveAvroSchemaFromFile(schemaFileLocation: String): Schema = {
    val avroSchemaFile = getClass.getResource(schemaFileLocation)
    val schema = new Parser().parse(new File(avroSchemaFile.toURI))
    schema
  }

  def convertToBytes(schema: Schema, record: GenericData.Record): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }
}
