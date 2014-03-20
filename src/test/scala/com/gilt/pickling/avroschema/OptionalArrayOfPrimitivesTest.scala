package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalArrayOfPrimitivesTest extends FunSuite with Assertions {

  //Some
  test("Generate schema from a case classwith a some array of int field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfInts.avsc") === fingerPrint(OptionArrayOfInts(Some(Array(123))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of long field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfLongs.avsc") === fingerPrint(OptionArrayOfLongs(Some(Array(123L))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of double field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfDoubles.avsc") === fingerPrint(OptionArrayOfDoubles(Some(Array(123D))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of float field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfFloats.avsc") === fingerPrint(OptionArrayOfFloats(Some(Array(123F))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of boolean field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfBooleans.avsc") === fingerPrint(OptionArrayOfBooleans(Some(Array(false, true))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of string field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfStrings.avsc") === fingerPrint(OptionArrayOfStrings(Some(Array("abc"))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of char field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfChars.avsc") === fingerPrint(OptionArrayOfChars(Some(Array('a'))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of byte field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfBytes.avsc") === fingerPrint(OptionArrayOfBytes(Some(Array(1.toByte))).pickle.value))
  }

  test("Generate schema from a case classwith a some array of short field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfShorts.avsc") === fingerPrint(OptionArrayOfShorts(Some(Array(1.toShort))).pickle.value))
  }

  //None
  test("Generate schema from a case class with a none array of int field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfInts.avsc") === fingerPrint(OptionArrayOfInts(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of long field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfLongs.avsc") === fingerPrint(OptionArrayOfLongs(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of double field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfDoubles.avsc") === fingerPrint(OptionArrayOfDoubles(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of float field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfFloats.avsc") === fingerPrint(OptionArrayOfFloats(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of boolean field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfBooleans.avsc") === fingerPrint(OptionArrayOfBooleans(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of string field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfStrings.avsc") === fingerPrint(OptionArrayOfStrings(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of char field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfChars.avsc") === fingerPrint(OptionArrayOfChars(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of byte field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfBytes.avsc") === fingerPrint(OptionArrayOfBytes(None).pickle.value))
  }

  test("Generate schema from a case class with a none array of short field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfShorts.avsc") === fingerPrint(OptionArrayOfShorts(None).pickle.value))
  }
}
