package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalMapOfPrimitivesTest extends FunSuite with Assertions {
  //negative 
  test("Generate schema from a case class only support a string type key") {
    intercept[PicklingException] {
      MapOfIntInts(Some(Map(12 -> 123))).pickle.value
    }
  }

  //some
  test("Generate schema from a case class with a some map of int field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfInts.avsc") === fingerPrint(OptionMapOfInts(Some(Map("abc" -> 123))).pickle.value))
  }

  test("Generate schema from a case class with a some map of long field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfLongs.avsc") === fingerPrint(OptionMapOfLongs(Some(Map("abc" -> 23L))).pickle.value))
  }

  test("Generate schema from a case class with a some map of double field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfDoubles.avsc") === fingerPrint(OptionMapOfDoubles(Some(Map("abc" -> 123D))).pickle.value))
  }

  test("Generate schema from a case class with a some map of float field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfFloats.avsc") === fingerPrint(OptionMapOfFloats(Some(Map("abc" -> 123F))).pickle.value))
  }

  test("Generate schema from a case class with a some map of boolean field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfBooleans.avsc") === fingerPrint(OptionMapOfBooleans(Some(Map("abc" -> false))).pickle.value))
  }

  test("Generate schema from a case class with a some map of string field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfStrings.avsc") === fingerPrint(OptionMapOfStrings(Some(Map("abc" -> "abc"))).pickle.value))
  }

  test("Generate schema from a case class with a some map of char field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfChars.avsc") === fingerPrint(OptionMapOfChars(Some(Map("abc" -> 'a'))).pickle.value))
  }

  test("Generate schema from a case class with a some map of byte field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfBytes.avsc") === fingerPrint(OptionMapOfBytes(Some(Map("abc" -> 1.toByte))).pickle.value))
  }

  test("Generate schema from a case class with a some map of short field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfShorts.avsc") === fingerPrint(OptionMapOfShorts(Some(Map("abc" -> 1.toShort))).pickle.value))
  }

  //None
  test("Generate schema from a case class with a none map of int field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfInts.avsc") === fingerPrint(OptionMapOfInts(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of long field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfLongs.avsc") === fingerPrint(OptionMapOfLongs(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of double field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfDoubles.avsc") === fingerPrint(OptionMapOfDoubles(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of float field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfFloats.avsc") === fingerPrint(OptionMapOfFloats(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of boolean field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfBooleans.avsc") === fingerPrint(OptionMapOfBooleans(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of string field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfStrings.avsc") === fingerPrint(OptionMapOfStrings(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of char field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfChars.avsc") === fingerPrint(OptionMapOfChars(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of byte field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfBytes.avsc") === fingerPrint(OptionMapOfBytes(None).pickle.value))
  }

  test("Generate schema from a case class with a none map of short field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfShorts.avsc") === fingerPrint(OptionMapOfShorts(None).pickle.value))
  }

}
