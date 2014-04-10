package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestUtils._
import com.gilt.pickling.TestObjs._
import scala.pickling._

class FingerprintListOfPrimitivesTest extends FunSuite with Assertions{

  test("Generate schema from a case class with a list of int field") {
    assert(Fingerprint(ListOfInts(List(123))) === fingerPrint(ListOfInts(List(123)).pickle.value))
  }

  test("Generate schema from a case class with a list of long field") {
    assert(Fingerprint(ListOfLongs(List(123L))) === fingerPrint(ListOfLongs(List(123L)).pickle.value))
  }

  test("Generate schema from a case class with a list of double field") {
    assert(Fingerprint(ListOfDoubles(List(123D))) === fingerPrint(ListOfDoubles(List(123D)).pickle.value))
  }

  test("Generate schema from a case class with a list of float field") {
    assert(Fingerprint(ListOfFloats(List(123F))) === fingerPrint(ListOfFloats(List(123F)).pickle.value))
  }

  test("Generate schema from a case class with a list of boolean field") {
    assert(Fingerprint(ListOfBooleans(List(false, true))) === fingerPrint(ListOfBooleans(List(false, true)).pickle.value))
  }

  test("Generate schema from a case class with a list of string field") {
    assert(Fingerprint(ListOfStrings(List("abc"))) === fingerPrint(ListOfStrings(List("abc")).pickle.value))
  }

  test("Generate schema from a case class with a list of char field") {
    assert(Fingerprint(ListOfChars(List('a'))) === fingerPrint(ListOfChars(List('a')).pickle.value))
  }

  test("Generate schema from a case class with a list of byte field") {
    assert(Fingerprint(ListOfBytes(List(1.toByte))) === fingerPrint(ListOfBytes(List(1.toByte)).pickle.value))
  }

  test("Generate schema from a case class with a list of short field") {
    assert(Fingerprint(ListOfShorts(List(1.toShort))) === fingerPrint(ListOfShorts(List(1.toShort)).pickle.value))
  }
}
