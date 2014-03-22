package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class SetOfPrimitivesTest extends FunSuite with Assertions {

  test("Generate schema from a case class with a set of int field") {
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(SetOfInts(Set(123)).pickle.value))
  }

  test("Generate schema from a case class with a set of long field") {
    assert(fingerPrint("/avro/sets/SetOfLongs.avsc") === fingerPrint(SetOfLongs(Set(123L)).pickle.value))
  }

  test("Generate schema from a case class with a set of double field") {
    assert(fingerPrint("/avro/sets/SetOfDoubles.avsc") === fingerPrint(SetOfDoubles(Set(123D)).pickle.value))
  }

  test("Generate schema from a case class with a set of float field") {
    assert(fingerPrint("/avro/sets/SetOfFloats.avsc") === fingerPrint(SetOfFloats(Set(123F)).pickle.value))
  }

  test("Generate schema from a case class with a set of boolean field") {
    assert(fingerPrint("/avro/sets/SetOfBooleans.avsc") === fingerPrint(SetOfBooleans(Set(false, true)).pickle.value))
  }

  test("Generate schema from a case class with a set of string field") {
    assert(fingerPrint("/avro/sets/SetOfStrings.avsc") === fingerPrint(SetOfStrings(Set("abc")).pickle.value))
  }

  test("Generate schema from a case class with a set of char field") {
    assert(fingerPrint("/avro/sets/SetOfChars.avsc") === fingerPrint(SetOfChars(Set('a')).pickle.value))
  }

  test("Generate schema from a case class with a set of byte field") {
    assert(fingerPrint("/avro/sets/SetOfBytes.avsc") === fingerPrint(SetOfBytes(Set(1.toByte)).pickle.value))
  }

  test("Generate schema from a case class with a set of short field") {
    assert(fingerPrint("/avro/sets/SetOfShorts.avsc") === fingerPrint(SetOfShorts(Set(1.toShort)).pickle.value))
  }

  test("Generate schema from a case class with an empty set of int field") {
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(SetOfInts(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of long field") {
    assert(fingerPrint("/avro/sets/SetOfLongs.avsc") === fingerPrint(SetOfLongs(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of double field") {
    assert(fingerPrint("/avro/sets/SetOfDoubles.avsc") === fingerPrint(SetOfDoubles(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of float field") {
    assert(fingerPrint("/avro/sets/SetOfFloats.avsc") === fingerPrint(SetOfFloats(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of boolean field") {
    assert(fingerPrint("/avro/sets/SetOfBooleans.avsc") === fingerPrint(SetOfBooleans(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of string field") {
    assert(fingerPrint("/avro/sets/SetOfStrings.avsc") === fingerPrint(SetOfStrings(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of char field") {
    assert(fingerPrint("/avro/sets/SetOfChars.avsc") === fingerPrint(SetOfChars(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of byte field") {
    assert(fingerPrint("/avro/sets/SetOfBytes.avsc") === fingerPrint(SetOfBytes(Set()).pickle.value))
  }

  test("Generate schema from a case class with an empty set of short field") {
    assert(fingerPrint("/avro/sets/SetOfShorts.avsc") === fingerPrint(SetOfShorts(Set()).pickle.value))
  }
}
