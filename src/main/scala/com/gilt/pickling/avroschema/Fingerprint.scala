package com.gilt.pickling.avroschema

import scala.pickling.{FastTypeTag, SPickler}
import org.apache.avro.Schema.Parser
import java.io.ByteArrayInputStream
import org.apache.avro.SchemaNormalization.parsingFingerprint

object Fingerprint {
  private val instanceSchema = new SchemaFingerprintGenerator()
  private val instanceObj = new CaseClassFingerprintGenerator()
  private val instanceJson = new JsonStringFingerprintGenerator()
  private val instanceJsonBytes = new JsonByteArrayFingerprintGenerator()

  def apply(json: String): Array[Byte] = instanceJson.fingerPrint(json)

  def apply(json: Array[Byte]): Array[Byte] = instanceJsonBytes.fingerPrint(json)

  @Deprecated
  def apply[T: SPickler : FastTypeTag](picklee: T): Array[Byte] = instanceObj.fingerPrintFromPicklee(picklee)

  def apply(schema: org.apache.avro.Schema): Array[Byte] = instanceSchema.fingerPrint(schema)
}

class CaseClassFingerprintGenerator extends SchemaFingerprintGenerator {
  def fingerPrintFromPicklee[T: SPickler : FastTypeTag](picklee: T): Array[Byte] = {
    import com.gilt.pickling.avroschema._
    import scala.pickling._
    fingerPrint(picklee.pickle.value)
  }

  private def fingerPrint(bytes: Array[Byte]): Array[Byte] = fingerPrint(buildSchemaFromBytes(bytes))
}

class JsonStringFingerprintGenerator extends SchemaFingerprintGenerator {
  def fingerPrint(json: String): Array[Byte] = fingerPrint(buildSchemaFromString(json))

  private def buildSchemaFromString(json: String): org.apache.avro.Schema =
    try {
      new Parser().parse(json)
    } catch {
      case e: Exception => throw new IllegalArgumentException(e)
    }
}

class JsonByteArrayFingerprintGenerator extends SchemaFingerprintGenerator {
  def fingerPrint(json: Array[Byte]): Array[Byte] = fingerPrint(buildSchemaFromBytes(json))
}

class SchemaFingerprintGenerator() {
  def fingerPrint(schema: org.apache.avro.Schema): Array[Byte] = parsingFingerprint("SHA-256", schema)

  protected def buildSchemaFromBytes(bytes: Array[Byte]): org.apache.avro.Schema =
    try {
      new Parser().parse(new ByteArrayInputStream(bytes))
    } catch {
      case e: Exception => throw new IllegalArgumentException(e)
    }
}