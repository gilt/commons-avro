package com.gilt.pickling.avroschema

import scala.pickling.{FastTypeTag, SPickler}
import org.apache.avro.Schema.Parser
import java.io.ByteArrayInputStream
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization.parsingFingerprint

object Fingerprint {
  private val instanceObj = new CaseClassFingerprintGenerator()
  private val instanceJson = new JsonStringFingerprintGenerator()
  private val instanceSchema = new SchemaFingerprintGenerator()

  def apply[T: SPickler : FastTypeTag](picklee: T): Array[Byte] = instanceObj.fingerPrintFromPicklee(picklee)

  def apply(json: String): Array[Byte] = instanceJson.fingerPrint(json)

  def apply(schema: Schema): Array[Byte] = instanceSchema.fingerPrint(schema)
}

class CaseClassFingerprintGenerator extends SchemaFingerprintGenerator {
  def fingerPrintFromPicklee[T: SPickler : FastTypeTag](picklee: T): Array[Byte] = {
    import com.gilt.pickling.avroschema._
    import scala.pickling._
    fingerPrint(picklee.pickle.value)
  }

  private def fingerPrint(bytes: Array[Byte]): Array[Byte] = fingerPrint(buildSchemaFromBytes(bytes))

  private def buildSchemaFromBytes(bytes: Array[Byte]): Schema =
    try {
      new Parser().parse(new ByteArrayInputStream(bytes))
    } catch {
      case e: Exception => throw new IllegalArgumentException(e)
    }
}

class JsonStringFingerprintGenerator extends SchemaFingerprintGenerator {
  def fingerPrint(json: String): Array[Byte] = fingerPrint(buildSchemaFromString(json))

  private def buildSchemaFromString(json: String): Schema =
    try {
      new Parser().parse(json)
    } catch {
      case e: Exception => throw new IllegalArgumentException(e)
    }
}

class SchemaFingerprintGenerator() {
  def fingerPrint(schema: Schema): Array[Byte] = parsingFingerprint("SHA-256", schema)
}