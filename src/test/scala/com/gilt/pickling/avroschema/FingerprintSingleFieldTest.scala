package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import com.gilt.pickling.TestUtils._

class FingerprintSingleFieldTest extends FunSuite with Assertions {

  test("Finger print for single int field") {
    assert(Fingerprint(SingleInt(123)) === fingerPrint(Schema[SingleInt]))
    assert(Fingerprint(Schema[SingleInt]) === fingerPrint(Schema[SingleInt]))
  }

  test("Finger print for single long field") {
    assert(Fingerprint(SingleLong(123)) === fingerPrint(Schema[SingleLong]))
    assert(Fingerprint(Schema[SingleLong]) === fingerPrint(Schema[SingleLong]))
  }

  test("Finger print for single double field") {
    assert(Fingerprint(SingleDouble(123)) === fingerPrint(Schema[SingleDouble]))
    assert(Fingerprint(Schema[SingleDouble]) === fingerPrint(Schema[SingleDouble]))
  }

  test("Finger print for single float field") {
    assert(Fingerprint(SingleFloat(123)) === fingerPrint(Schema[SingleFloat]))
    assert(Fingerprint(Schema[SingleFloat]) === fingerPrint(Schema[SingleFloat]))
  }

  test("Finger print for single boolean field") {
    assert(Fingerprint(SingleBoolean(true)) === fingerPrint(Schema[SingleBoolean]))
    assert(Fingerprint(Schema[SingleBoolean]) === fingerPrint(Schema[SingleBoolean]))
  }

  test("Finger print for single string field") {
    assert(Fingerprint(SingleString("abc")) === fingerPrint(Schema[SingleString]))
    assert(Fingerprint(Schema[SingleString]) === fingerPrint(Schema[SingleString]))
  }

  test("Finger print for single char field") {
    assert(Fingerprint(SingleChar('a')) === fingerPrint(Schema[SingleChar]))
    assert(Fingerprint(Schema[SingleChar]) === fingerPrint(Schema[SingleChar]))
  }

  test("Finger print for single byte field") {
    assert(Fingerprint(SingleByte(1.toByte)) === fingerPrint(Schema[SingleByte]))
    assert(Fingerprint(Schema[SingleByte]) === fingerPrint(Schema[SingleByte]))
  }

  test("Finger print for single short field") {
    assert(Fingerprint(SingleShort(1.toShort)) === fingerPrint(Schema[SingleShort]))
    assert(Fingerprint(Schema[SingleShort]) === fingerPrint(Schema[SingleShort]))
  }
}
