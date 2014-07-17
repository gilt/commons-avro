package com.gilt.pickling.avroschema

import org.apache.avro.Schema.Parser
import java.io.ByteArrayInputStream
import org.apache.avro.SchemaNormalization.parsingFingerprint
import scala.reflect.runtime.universe.TypeTag

object Fingerprint {
  private val instanceSchema = new SchemaFingerprintGenerator()
  private val instanceJsonBytes = new JsonByteArrayFingerprintGenerator()

  def apply(json: String): Array[Byte] = apply(json.getBytes)

  def apply(json: Array[Byte]): Array[Byte] = instanceJsonBytes.fingerPrint(json)

  def apply[T](obj: T)(implicit ttag: TypeTag[T]): Array[Byte] = apply(Schema[T])

  def apply(schema: org.apache.avro.Schema): Array[Byte] = instanceSchema.fingerPrint(schema)
}

class JsonByteArrayFingerprintGenerator extends SchemaFingerprintGenerator {
  def fingerPrint(json: Array[Byte]): Array[Byte] = fingerPrint(buildSchemaFromBytes(json))
  protected def buildSchemaFromBytes(bytes: Array[Byte]): org.apache.avro.Schema =
    try {
      new Parser().parse(new ByteArrayInputStream(bytes))
    } catch {
      case e: Exception => throw new IllegalArgumentException(e)
    }
}

class SchemaFingerprintGenerator() {
  def fingerPrint(schema: org.apache.avro.Schema): Array[Byte] = parsingFingerprint("SHA-256", schema)
}