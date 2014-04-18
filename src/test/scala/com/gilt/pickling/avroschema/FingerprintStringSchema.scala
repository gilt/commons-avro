package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs.SingleInt

class FingerprintStringSchema extends FunSuite with Assertions {
  test("Finger print for a schema as a string json ") {
    import com.gilt.pickling.avroschema._
    import scala.pickling._
    assert(Fingerprint(SingleInt(123)) === Fingerprint(new String(SingleInt(123).pickle.value)))
  }

  test("An empty string") {
    intercept[IllegalArgumentException](Fingerprint(""))
  }

  test("A null value") {
    intercept[IllegalArgumentException] {
      val s: String = null
      Fingerprint(s)
    }
  }

  test("A badly formatted json") {
    intercept[IllegalArgumentException] {
      Fingerprint("badly formatted json")
    }
  }
}
