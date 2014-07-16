package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalListOfPrimitivesTest extends FunSuite with Assertions {

  //some
  test("Generate schema from a case class with a some list of int field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfInts.avsc") === fingerPrint(OptionListOfInts(Some(List(123))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfInts.avsc") === fingerPrint(Schema(OptionListOfInts(Some(List(123))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfInts.avsc") === fingerPrint(Schema(classOf[OptionListOfInts])))
    assert(fingerPrint("/avro/option-lists/OptionListOfInts.avsc") === fingerPrint(Schema[OptionListOfInts]))
  }

  test("Generate schema from a case class with a some list of long field") {
    //assert(fingerPrint("/avro/option-lists/OptionListOfLongs.avsc") === fingerPrint(OptionListOfLongs(Some(List(123L))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfLongs.avsc") === fingerPrint(Schema(OptionListOfLongs(Some(List(123L))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfLongs.avsc") === fingerPrint(Schema(classOf[OptionListOfLongs])))
    assert(fingerPrint("/avro/option-lists/OptionListOfLongs.avsc") === fingerPrint(Schema[OptionListOfLongs]))
  }

  test("Generate schema from a case class with a some list of double field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfDoubles.avsc") === fingerPrint(OptionListOfDoubles(Some(List(123D))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfDoubles.avsc") === fingerPrint(Schema(OptionListOfDoubles(Some(List(123D))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfDoubles.avsc") === fingerPrint(Schema(classOf[OptionListOfDoubles])))
    assert(fingerPrint("/avro/option-lists/OptionListOfDoubles.avsc") === fingerPrint(Schema[OptionListOfDoubles]))
  }

  test("Generate schema from a case class with a some list of float field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfFloats.avsc") === fingerPrint(OptionListOfFloats(Some(List(123F))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfFloats.avsc") === fingerPrint(Schema(OptionListOfFloats(Some(List(123F))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfFloats.avsc") === fingerPrint(Schema(classOf[OptionListOfFloats])))
    assert(fingerPrint("/avro/option-lists/OptionListOfFloats.avsc") === fingerPrint(Schema[OptionListOfFloats]))
  }

  test("Generate schema from a case class with a some list of boolean field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfBooleans.avsc") === fingerPrint(OptionListOfBooleans(Some(List(false, true))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfBooleans.avsc") === fingerPrint(Schema(OptionListOfBooleans(Some(List(false, true))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfBooleans.avsc") === fingerPrint(Schema(classOf[OptionListOfBooleans])))
    assert(fingerPrint("/avro/option-lists/OptionListOfBooleans.avsc") === fingerPrint(Schema[OptionListOfBooleans]))
  }

  test("Generate schema from a case class with a some list of string field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfStrings.avsc") === fingerPrint(OptionListOfStrings(Some(List("abc"))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfStrings.avsc") === fingerPrint(Schema(OptionListOfStrings(Some(List("abc"))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfStrings.avsc") === fingerPrint(Schema(classOf[OptionListOfStrings])))
    assert(fingerPrint("/avro/option-lists/OptionListOfStrings.avsc") === fingerPrint(Schema[OptionListOfStrings]))
  }

  test("Generate schema from a case class with a some list of char field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfChars.avsc") === fingerPrint(OptionListOfChars(Some(List('a'))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfChars.avsc") === fingerPrint(Schema(OptionListOfChars(Some(List('a'))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfChars.avsc") === fingerPrint(Schema(classOf[OptionListOfChars])))
    assert(fingerPrint("/avro/option-lists/OptionListOfChars.avsc") === fingerPrint(Schema[OptionListOfChars]))
  }

  test("Generate schema from a case class with a some list of byte field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfBytes.avsc") === fingerPrint(OptionListOfBytes(Some(List(1.toByte))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfBytes.avsc") === fingerPrint(Schema(OptionListOfBytes(Some(List(1.toByte))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfBytes.avsc") === fingerPrint(Schema(classOf[OptionListOfBytes])))
    assert(fingerPrint("/avro/option-lists/OptionListOfBytes.avsc") === fingerPrint(Schema[OptionListOfBytes]))
  }

  test("Generate schema from a case class with a some list of short field") {
    assert(fingerPrint("/avro/option-lists/OptionListOfShorts.avsc") === fingerPrint(OptionListOfShorts(Some(List(1.toShort))).pickle.value))
    assert(fingerPrint("/avro/option-lists/OptionListOfShorts.avsc") === fingerPrint(Schema(OptionListOfShorts(Some(List(1.toShort))))))
    assert(fingerPrint("/avro/option-lists/OptionListOfShorts.avsc") === fingerPrint(Schema(classOf[OptionListOfShorts])))
    assert(fingerPrint("/avro/option-lists/OptionListOfShorts.avsc") === fingerPrint(Schema[OptionListOfShorts]))
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
