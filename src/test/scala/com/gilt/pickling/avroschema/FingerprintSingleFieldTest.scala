package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class FingerprintSingleFieldTest extends FunSuite with Assertions {

  test("Finger print for single int field") {
    assert(Fingerprint(SingleInt(123)) === fingerPrint(SingleInt(123).pickle.value))
  }

  test("Finger print for single long field") {
    assert(Fingerprint(SingleLong(123)) === fingerPrint(SingleLong(123).pickle.value))
  }

  test("Finger print for single double field") {
    assert(Fingerprint(SingleDouble(123)) === fingerPrint(SingleDouble(123).pickle.value))
  }

  test("Finger print for single float field") {
    assert(Fingerprint(SingleFloat(123)) === fingerPrint(SingleFloat(123).pickle.value))
  }

  test("Finger print for single boolean field") {
    assert(Fingerprint(SingleBoolean(true)) === fingerPrint(SingleBoolean(id = true).pickle.value))
  }

  test("Finger print for single string field") {
    assert(Fingerprint(SingleString("abc")) === fingerPrint(SingleString("abc").pickle.value))
  }

  test("Finger print for single char field") {
    assert(Fingerprint(SingleChar('a')) === fingerPrint(SingleChar('a').pickle.value))
  }

  test("Finger print for single byte field") {
    assert(Fingerprint(SingleByte(1.toByte)) === fingerPrint(SingleByte(1.toByte).pickle.value))
  }

  test("Finger print for single short field") {
    assert(Fingerprint(SingleShort(1.toShort)) === fingerPrint(SingleShort(1.toShort).pickle.value))
  }
}
