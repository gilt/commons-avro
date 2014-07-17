package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestUtils._
import com.gilt.pickling.TestObjs._

class FingerprintListOfPrimitivesTest extends FunSuite with Assertions{

  test("Generate schema from a case class with a list of int field") {
    assert(Fingerprint(ListOfInts(List(123))) === fingerPrint(Schema[ListOfInts]))
  }

  test("Generate schema from a case class with a list of long field") {
    assert(Fingerprint(ListOfLongs(List(123L))) === fingerPrint(Schema[ListOfLongs]))
  }

  test("Generate schema from a case class with a list of double field") {
    assert(Fingerprint(ListOfDoubles(List(123D))) === fingerPrint(Schema[ListOfDoubles]))
  }

  test("Generate schema from a case class with a list of float field") {
    assert(Fingerprint(ListOfFloats(List(123F))) === fingerPrint(Schema[ListOfFloats]))
  }

  test("Generate schema from a case class with a list of boolean field") {
    assert(Fingerprint(ListOfBooleans(List(false, true))) === fingerPrint(Schema[ListOfBooleans]))
  }

  test("Generate schema from a case class with a list of string field") {
    assert(Fingerprint(ListOfStrings(List("abc"))) === fingerPrint(Schema[ListOfStrings]))
  }

  test("Generate schema from a case class with a list of char field") {
    assert(Fingerprint(ListOfChars(List('a'))) === fingerPrint(Schema[ListOfChars]))
  }

  test("Generate schema from a case class with a list of byte field") {
    assert(Fingerprint(ListOfBytes(List(1.toByte))) === fingerPrint(Schema[ListOfBytes]))
  }

  test("Generate schema from a case class with a list of short field") {
    assert(Fingerprint(ListOfShorts(List(1.toShort))) === fingerPrint(Schema[ListOfShorts]))
  }
}
