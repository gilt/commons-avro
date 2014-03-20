package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalSetOfPrimitivesTest extends FunSuite with Assertions {

  //Some
  test("Generate schema from a case class with a some set of int field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfInts.avsc") === fingerPrint(OptionSetOfInts(Some(Set(123))).pickle.value))
  }

  test("Generate schema from a case class with a some set of long field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfLongs.avsc") === fingerPrint(OptionSetOfLongs(Some(Set(123L))).pickle.value))
  }

  test("Generate schema from a case class with a some set of double field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfDoubles.avsc") === fingerPrint(OptionSetOfDoubles(Some(Set(123D))).pickle.value))
  }

  test("Generate schema from a case class with a some set of float field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfFloats.avsc") === fingerPrint(OptionSetOfFloats(Some(Set(123F))).pickle.value))
  }

  test("Generate schema from a case class with a some set of boolean field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfBooleans.avsc") === fingerPrint(OptionSetOfBooleans(Some(Set(false, true))).pickle.value))
  }

  test("Generate schema from a case class with a some set of string field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfStrings.avsc") === fingerPrint(OptionSetOfStrings(Some(Set("abc"))).pickle.value))
  }

  test("Generate schema from a case class with a some set of char field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfChars.avsc") === fingerPrint(OptionSetOfChars(Some(Set('a'))).pickle.value))
  }

  test("Generate schema from a case class with a some set of byte field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfBytes.avsc") === fingerPrint(OptionSetOfBytes(Some(Set(1.toByte))).pickle.value))
  }

  test("Generate schema from a case class with a some set of short field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfShorts.avsc") === fingerPrint(OptionSetOfShorts(Some(Set(1.toShort))).pickle.value))
  }

  //None
  test("Generate schema from a case class with a none set of int field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfInts.avsc") === fingerPrint(OptionSetOfInts(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of long field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfLongs.avsc") === fingerPrint(OptionSetOfLongs(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of double field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfDoubles.avsc") === fingerPrint(OptionSetOfDoubles(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of float field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfFloats.avsc") === fingerPrint(OptionSetOfFloats(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of boolean field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfBooleans.avsc") === fingerPrint(OptionSetOfBooleans(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of string field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfStrings.avsc") === fingerPrint(OptionSetOfStrings(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of char field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfChars.avsc") === fingerPrint(OptionSetOfChars(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of byte field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfBytes.avsc") === fingerPrint(OptionSetOfBytes(None).pickle.value))
  }

  test("Generate schema from a case class with a none set of short field") {
    assert(fingerPrint("/avro/option-sets/OptionSetOfShorts.avsc") === fingerPrint(OptionSetOfShorts(None).pickle.value))
  }
}
