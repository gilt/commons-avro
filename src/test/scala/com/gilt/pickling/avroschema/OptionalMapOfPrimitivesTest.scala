package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class OptionalMapOfPrimitivesTest extends FunSuite with Assertions {
  //negative 
  test("Generate schema from a case class only support a string type key") {
    intercept[PicklingException] { OptionMapOfIntInts(Some(Map(12 -> 123))).pickle.value }
    intercept[IllegalArgumentException] { Schema(OptionMapOfIntInts(Some(Map(12 -> 123)))) }
    intercept[IllegalArgumentException] { Schema(classOf[OptionMapOfIntInts]) }
    intercept[IllegalArgumentException] { Schema[OptionMapOfIntInts] }
  }

  //some
  test("Generate schema from a case class with a some map of int field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfInts.avsc") === fingerPrint(OptionMapOfInts(Some(Map("abc" -> 123))).pickle.value))
  }

  test("Generate schema from a case class with a some map of long field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfLongs.avsc") === fingerPrint(OptionMapOfLongs(Some(Map("abc" -> 23L))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfLongs.avsc") === fingerPrint(Schema(OptionMapOfLongs(Some(Map("abc" -> 23L))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfLongs.avsc") === fingerPrint(Schema(classOf[OptionMapOfLongs])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfLongs.avsc") === fingerPrint(Schema[OptionMapOfLongs]))
  }

  test("Generate schema from a case class with a some map of double field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfDoubles.avsc") === fingerPrint(OptionMapOfDoubles(Some(Map("abc" -> 123D))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfDoubles.avsc") === fingerPrint(Schema(OptionMapOfDoubles(Some(Map("abc" -> 123D))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfDoubles.avsc") === fingerPrint(Schema(classOf[OptionMapOfDoubles])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfDoubles.avsc") === fingerPrint(Schema[OptionMapOfDoubles]))
  }

  test("Generate schema from a case class with a some map of float field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfFloats.avsc") === fingerPrint(OptionMapOfFloats(Some(Map("abc" -> 123F))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfFloats.avsc") === fingerPrint(Schema(OptionMapOfFloats(Some(Map("abc" -> 123F))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfFloats.avsc") === fingerPrint(Schema(classOf[OptionMapOfFloats])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfFloats.avsc") === fingerPrint(Schema[OptionMapOfFloats]))
  }

  test("Generate schema from a case class with a some map of boolean field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfBooleans.avsc") === fingerPrint(OptionMapOfBooleans(Some(Map("abc" -> false))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfBooleans.avsc") === fingerPrint(Schema(OptionMapOfBooleans(Some(Map("abc" -> false))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfBooleans.avsc") === fingerPrint(Schema(classOf[OptionMapOfBooleans])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfBooleans.avsc") === fingerPrint(Schema[OptionMapOfBooleans]))
  }

  test("Generate schema from a case class with a some map of string field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfStrings.avsc") === fingerPrint(OptionMapOfStrings(Some(Map("abc" -> "abc"))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfStrings.avsc") === fingerPrint(Schema(OptionMapOfStrings(Some(Map("abc" -> "abc"))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfStrings.avsc") === fingerPrint(Schema(classOf[OptionMapOfStrings])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfStrings.avsc") === fingerPrint(Schema[OptionMapOfStrings]))
  }

  test("Generate schema from a case class with a some map of char field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfChars.avsc") === fingerPrint(OptionMapOfChars(Some(Map("abc" -> 'a'))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfChars.avsc") === fingerPrint(Schema(OptionMapOfChars(Some(Map("abc" -> 'a'))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfChars.avsc") === fingerPrint(Schema(classOf[OptionMapOfChars])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfChars.avsc") === fingerPrint(Schema[OptionMapOfChars]))
  }

  test("Generate schema from a case class with a some map of byte field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfBytes.avsc") === fingerPrint(OptionMapOfBytes(Some(Map("abc" -> 1.toByte))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfBytes.avsc") === fingerPrint(Schema(OptionMapOfBytes(Some(Map("abc" -> 1.toByte))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfBytes.avsc") === fingerPrint(Schema(classOf[OptionMapOfBytes])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfBytes.avsc") === fingerPrint(Schema[OptionMapOfBytes]))
  }

  test("Generate schema from a case class with a some map of short field") {
    assert(fingerPrint("/avro/option-maps/OptionMapOfShorts.avsc") === fingerPrint(OptionMapOfShorts(Some(Map("abc" -> 1.toShort))).pickle.value))
    assert(fingerPrint("/avro/option-maps/OptionMapOfShorts.avsc") === fingerPrint(Schema(OptionMapOfShorts(Some(Map("abc" -> 1.toShort))))))
    assert(fingerPrint("/avro/option-maps/OptionMapOfShorts.avsc") === fingerPrint(Schema(classOf[OptionMapOfShorts])))
    assert(fingerPrint("/avro/option-maps/OptionMapOfShorts.avsc") === fingerPrint(Schema[OptionMapOfShorts]))
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
