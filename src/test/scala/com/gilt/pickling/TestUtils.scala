package com.gilt.pickling

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import org.apache.avro.generic.{GenericRecord, GenericDatumWriter, GenericData}
import org.apache.avro.io.{EncoderFactory, BinaryEncoder}
import org.apache.avro.SchemaNormalization._

object TestUtils {

  def retrieveAvroSchemaFromFile(schemaFileLocation: String): Schema = {
    val avroSchemaFile = getClass.getResource(schemaFileLocation)
    val schema = new Parser().parse(new File(avroSchemaFile.toURI))
    schema
  }

  def convertToBytes(schema: Schema, record: GenericData.Record): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val encoder: BinaryEncoder = EncoderFactory.get().directBinaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

  def fingerPrint(bytes: Array[Byte]): Array[Byte] = fingerPrint(new Parser().parse(new ByteArrayInputStream(bytes)))

  def fingerPrint(filename:String): Array[Byte] = fingerPrint(retrieveAvroSchemaFromFile(filename))

  def fingerPrint(schema: Schema) : Array[Byte] = parsingFingerprint("SHA-256", schema)

}
