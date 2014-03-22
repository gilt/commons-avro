package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class ListOfPrimitivesTest extends FunSuite with Assertions {

  test("Generate schema from a case class with a list of int field") {
    assert(fingerPrint("/avro/lists/ListOfInts.avsc") === fingerPrint(ListOfInts(List(123)).pickle.value))
  }

  test("Generate schema from a case class with a list of long field") {
    assert(fingerPrint("/avro/lists/ListOfLongs.avsc") === fingerPrint(ListOfLongs(List(123L)).pickle.value))
  }

  test("Generate schema from a case class with a list of double field") {
    assert(fingerPrint("/avro/lists/ListOfDoubles.avsc") === fingerPrint(ListOfDoubles(List(123D)).pickle.value))
  }

  test("Generate schema from a case class with a list of float field") {
    assert(fingerPrint("/avro/lists/ListOfFloats.avsc") === fingerPrint(ListOfFloats(List(123F)).pickle.value))
  }

  test("Generate schema from a case class with a list of boolean field") {
    assert(fingerPrint("/avro/lists/ListOfBooleans.avsc") === fingerPrint(ListOfBooleans(List(false, true)).pickle.value))
  }

  test("Generate schema from a case class with a list of string field") {
    assert(fingerPrint("/avro/lists/ListOfStrings.avsc") === fingerPrint(ListOfStrings(List("abc")).pickle.value))
  }

  test("Generate schema from a case class with a list of char field") {
    assert(fingerPrint("/avro/lists/ListOfChars.avsc") === fingerPrint(ListOfChars(List('a')).pickle.value))
  }

  test("Generate schema from a case class with a list of byte field") {
    assert(fingerPrint("/avro/lists/ListOfBytes.avsc") === fingerPrint(ListOfBytes(List(1.toByte)).pickle.value))
  }

  test("Generate schema from a case class with a list of short field") {
    assert(fingerPrint("/avro/lists/ListOfShorts.avsc") === fingerPrint(ListOfShorts(List(1.toShort)).pickle.value))
  }

  test("Generate schema from a case class with an empty list of int field") {
    assert(fingerPrint("/avro/lists/ListOfInts.avsc") === fingerPrint(ListOfInts(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of long field") {
    assert(fingerPrint("/avro/lists/ListOfLongs.avsc") === fingerPrint(ListOfLongs(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of double field") {
    assert(fingerPrint("/avro/lists/ListOfDoubles.avsc") === fingerPrint(ListOfDoubles(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of float field") {
    assert(fingerPrint("/avro/lists/ListOfFloats.avsc") === fingerPrint(ListOfFloats(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of boolean field") {
    assert(fingerPrint("/avro/lists/ListOfBooleans.avsc") === fingerPrint(ListOfBooleans(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of string field") {
    assert(fingerPrint("/avro/lists/ListOfStrings.avsc") === fingerPrint(ListOfStrings(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of char field") {
    assert(fingerPrint("/avro/lists/ListOfChars.avsc") === fingerPrint(ListOfChars(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of byte field") {
    assert(fingerPrint("/avro/lists/ListOfBytes.avsc") === fingerPrint(ListOfBytes(List()).pickle.value))
  }

  test("Generate schema from a case class with an empty list of short field") {
    assert(fingerPrint("/avro/lists/ListOfShorts.avsc") === fingerPrint(ListOfShorts(List()).pickle.value))
  }

}
