package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs.SingleInt

class FingerprintStringSchema extends FunSuite with Assertions {
  test("Finger print for a schema as a string json ") {
    assert(Fingerprint(SingleInt(123)) === Fingerprint(new String(Schema[SingleInt])))
  }

  test("An empty string") {
    intercept[IllegalArgumentException](Fingerprint(""))
  }

  test("A badly formatted json") {
    intercept[IllegalArgumentException] (Fingerprint("badly formatted json"))
  }
}
