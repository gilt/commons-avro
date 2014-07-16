package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalArrayOfPrimitivesTest extends FunSuite with Assertions {

  //Some
  test("Generate schema from a case classwith a some array of int field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfInts.avsc") === fingerPrint(OptionArrayOfInts(Some(Array(123))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfInts.avsc") === fingerPrint(Schema(OptionArrayOfInts(Some(Array(123))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfInts.avsc") === fingerPrint(Schema(classOf[OptionArrayOfInts])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfInts.avsc") === fingerPrint(Schema[OptionArrayOfInts]))
  }

  test("Generate schema from a case classwith a some array of long field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfLongs.avsc") === fingerPrint(OptionArrayOfLongs(Some(Array(123L))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfLongs.avsc") === fingerPrint(Schema(OptionArrayOfLongs(Some(Array(123L))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfLongs.avsc") === fingerPrint(Schema(classOf[OptionArrayOfLongs])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfLongs.avsc") === fingerPrint(Schema[OptionArrayOfLongs]))
  }

  test("Generate schema from a case classwith a some array of double field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfDoubles.avsc") === fingerPrint(OptionArrayOfDoubles(Some(Array(123D))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfDoubles.avsc") === fingerPrint(Schema(OptionArrayOfDoubles(Some(Array(123D))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfDoubles.avsc") === fingerPrint(Schema(classOf[OptionArrayOfDoubles])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfDoubles.avsc") === fingerPrint(Schema[OptionArrayOfDoubles]))
  }

  test("Generate schema from a case classwith a some array of float field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfFloats.avsc") === fingerPrint(OptionArrayOfFloats(Some(Array(123F))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfFloats.avsc") === fingerPrint(Schema(OptionArrayOfFloats(Some(Array(123F))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfFloats.avsc") === fingerPrint(Schema(classOf[OptionArrayOfFloats])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfFloats.avsc") === fingerPrint(Schema[OptionArrayOfFloats]))
  }

  test("Generate schema from a case classwith a some array of boolean field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfBooleans.avsc") === fingerPrint(OptionArrayOfBooleans(Some(Array(false, true))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfBooleans.avsc") === fingerPrint(Schema(OptionArrayOfBooleans(Some(Array(false, true))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfBooleans.avsc") === fingerPrint(Schema(classOf[OptionArrayOfBooleans])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfBooleans.avsc") === fingerPrint(Schema[OptionArrayOfBooleans]))
  }

  test("Generate schema from a case classwith a some array of string field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfStrings.avsc") === fingerPrint(OptionArrayOfStrings(Some(Array("abc"))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfStrings.avsc") === fingerPrint(Schema(OptionArrayOfStrings(Some(Array("abc"))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfStrings.avsc") === fingerPrint(Schema(classOf[OptionArrayOfStrings])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfStrings.avsc") === fingerPrint(Schema[OptionArrayOfStrings]))
  }

  test("Generate schema from a case classwith a some array of char field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfChars.avsc") === fingerPrint(OptionArrayOfChars(Some(Array('a'))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfChars.avsc") === fingerPrint(Schema(OptionArrayOfChars(Some(Array('a'))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfChars.avsc") === fingerPrint(Schema(classOf[OptionArrayOfChars])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfChars.avsc") === fingerPrint(Schema[OptionArrayOfChars]))
  }

  test("Generate schema from a case classwith a some array of byte field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfBytes.avsc") === fingerPrint(OptionArrayOfBytes(Some(Array(1.toByte))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfBytes.avsc") === fingerPrint(Schema(OptionArrayOfBytes(Some(Array(1.toByte))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfBytes.avsc") === fingerPrint(Schema(classOf[OptionArrayOfBytes])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfBytes.avsc") === fingerPrint(Schema[OptionArrayOfBytes]))
  }

  test("Generate schema from a case classwith a some array of short field") {
    assert(fingerPrint("/avro/option-array/OptionArrayOfShorts.avsc") === fingerPrint(OptionArrayOfShorts(Some(Array(1.toShort))).pickle.value))
    assert(fingerPrint("/avro/option-array/OptionArrayOfShorts.avsc") === fingerPrint(Schema(OptionArrayOfShorts(Some(Array(1.toShort))))))
    assert(fingerPrint("/avro/option-array/OptionArrayOfShorts.avsc") === fingerPrint(Schema(classOf[OptionArrayOfShorts])))
    assert(fingerPrint("/avro/option-array/OptionArrayOfShorts.avsc") === fingerPrint(Schema[OptionArrayOfShorts]))
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
