package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalListOfPrimitivesTest extends FunSuite with Assertions {

  //some
  test("Generate schema from a case class with a some list of int field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfInts.avsc") === fingerPrint(OptionListOfInts(Some(List(123))).pickle.value))
  }

  //TODO does not want to compile for some reason - filename too long
  //  test("Generate schema from a case class with a some list of long field") {
  //    val print = fingerPrint(OptionListOfLongs(Some(List(123L))).pickle.value)
  //    assert(fingerPrint("/avro/option-lists/OptionListOfLongs.avsc") === print)
  //  }

  test("Generate schema from a case class with a some list of double field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfDoubles.avsc") === fingerPrint(OptionListOfDoubles(Some(List(123D))).pickle.value))
  }

  test("Generate schema from a case class with a some list of float field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfFloats.avsc") === fingerPrint(OptionListOfFloats(Some(List(123F))).pickle.value))
  }

  test("Generate schema from a case class with a some list of boolean field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfBooleans.avsc") === fingerPrint(OptionListOfBooleans(Some(List(false, true))).pickle.value))
  }

  test("Generate schema from a case class with a some list of string field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfStrings.avsc") === fingerPrint(OptionListOfStrings(Some(List("abc"))).pickle.value))
  }

  test("Generate schema from a case class with a some list of char field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfChars.avsc") === fingerPrint(OptionListOfChars(Some(List('a'))).pickle.value))
  }

  test("Generate schema from a case class with a some list of byte field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfBytes.avsc") === fingerPrint(OptionListOfBytes(Some(List(1.toByte))).pickle.value))
  }

  test("Generate schema from a case class with a some list of short field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfShorts.avsc") === fingerPrint(OptionListOfShorts(Some(List(1.toShort))).pickle.value))
  }

  //None
  test("Generate schema from a case class with a none list of int field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfInts.avsc") === fingerPrint(OptionListOfInts(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of long field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfLongs.avsc") === fingerPrint(OptionListOfLongs(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of double field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfDoubles.avsc") === fingerPrint(OptionListOfDoubles(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of float field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfFloats.avsc") === fingerPrint(OptionListOfFloats(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of boolean field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfBooleans.avsc") === fingerPrint(OptionListOfBooleans(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of string field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfStrings.avsc") === fingerPrint(OptionListOfStrings(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of char field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfChars.avsc") === fingerPrint(OptionListOfChars(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of byte field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfBytes.avsc") === fingerPrint(OptionListOfBytes(None).pickle.value))
  }

  test("Generate schema from a case class with a none list of short field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfShorts.avsc") === fingerPrint(OptionListOfShorts(None).pickle.value))
  }

}
