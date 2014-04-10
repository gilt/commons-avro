package com.gilt.pickling.avroschema

import scala.pickling.{FastTypeTag, SPickler}
import org.apache.avro.Schema.Parser
import java.io.ByteArrayInputStream
import org.apache.avro.Schema
import org.apache.avro.SchemaNormalization._

object Fingerprint {
  private val instance = new FingerprintGenerator()
  def apply[T: SPickler : FastTypeTag](picklee: T): Array[Byte] = instance.generate(picklee)
}

class FingerprintGenerator(){
  def generate[T: SPickler : FastTypeTag](picklee: T) : Array[Byte] = {
    import com.gilt.pickling.avroschema._
    import scala.pickling._
    fingerPrint(picklee.pickle.value)
  }

  private def fingerPrint(bytes: Array[Byte]): Array[Byte] = fingerPrint(new Parser().parse(new ByteArrayInputStream(bytes)))

  private def fingerPrint(schema: Schema): Array[Byte] = parsingFingerprint("SHA-256", schema)
}
