package com.gilt.pickling.avroschema

import com.gilt.pickling.TestObjs.SingleInt
import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema.Parser
import java.io.ByteArrayInputStream

class FingerprintSchema extends FunSuite with Assertions{
  test("Finger print for a schema as a string json ") {
    import com.gilt.pickling.avroschema._
    import scala.pickling._

    val schema = new Parser().parse(new ByteArrayInputStream(SingleInt(123).pickle.value))
    assert(Fingerprint(SingleInt(123)) === Fingerprint(schema))
  }
}
