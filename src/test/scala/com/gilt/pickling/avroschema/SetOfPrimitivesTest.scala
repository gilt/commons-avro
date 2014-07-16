package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class SetOfPrimitivesTest extends FunSuite with Assertions {

  test("Generate schema from a case class with a set of int field") {
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(SetOfInts(Set(123)).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(Schema(SetOfInts(Set(123)))))
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(Schema(classOf[SetOfInts])))
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(Schema[SetOfInts]))
  }

  test("Generate schema from a case class with a set of long field") {
    assert(fingerPrint("/avro/sets/SetOfLongs.avsc") === fingerPrint(SetOfLongs(Set(123L)).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfLongs.avsc") === fingerPrint(Schema(SetOfLongs(Set(123L)))))
    assert(fingerPrint("/avro/sets/SetOfLongs.avsc") === fingerPrint(Schema(classOf[SetOfLongs])))
    assert(fingerPrint("/avro/sets/SetOfLongs.avsc") === fingerPrint(Schema[SetOfLongs]))
  }

  test("Generate schema from a case class with a set of double field") {
    assert(fingerPrint("/avro/sets/SetOfDoubles.avsc") === fingerPrint(SetOfDoubles(Set(123D)).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(Schema(SetOfInts(Set(123)))))
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(Schema(classOf[SetOfInts])))
    assert(fingerPrint("/avro/sets/SetOfInts.avsc") === fingerPrint(Schema[SetOfInts]))
  }

  test("Generate schema from a case class with a set of float field") {
    assert(fingerPrint("/avro/sets/SetOfFloats.avsc") === fingerPrint(SetOfFloats(Set(123F)).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfFloats.avsc") === fingerPrint(Schema(SetOfFloats(Set(123F)))))
    assert(fingerPrint("/avro/sets/SetOfFloats.avsc") === fingerPrint(Schema(classOf[SetOfFloats])))
    assert(fingerPrint("/avro/sets/SetOfFloats.avsc") === fingerPrint(Schema[SetOfFloats]))
  }

  test("Generate schema from a case class with a set of boolean field") {
    assert(fingerPrint("/avro/sets/SetOfBooleans.avsc") === fingerPrint(SetOfBooleans(Set(false, true)).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfBooleans.avsc") === fingerPrint(Schema(SetOfBooleans(Set(false, true)))))
    assert(fingerPrint("/avro/sets/SetOfBooleans.avsc") === fingerPrint(Schema(classOf[SetOfBooleans])))
    assert(fingerPrint("/avro/sets/SetOfBooleans.avsc") === fingerPrint(Schema[SetOfBooleans]))
  }

  test("Generate schema from a case class with a set of string field") {
    assert(fingerPrint("/avro/sets/SetOfStrings.avsc") === fingerPrint(SetOfStrings(Set("abc")).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfStrings.avsc") === fingerPrint(Schema(SetOfStrings(Set("abc")))))
    assert(fingerPrint("/avro/sets/SetOfStrings.avsc") === fingerPrint(Schema(classOf[SetOfStrings])))
    assert(fingerPrint("/avro/sets/SetOfStrings.avsc") === fingerPrint(Schema[SetOfStrings]))
  }

  test("Generate schema from a case class with a set of char field") {
    assert(fingerPrint("/avro/sets/SetOfChars.avsc") === fingerPrint(SetOfChars(Set('a')).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfChars.avsc") === fingerPrint(Schema(SetOfChars(Set('a')))))
    assert(fingerPrint("/avro/sets/SetOfChars.avsc") === fingerPrint(Schema(classOf[SetOfChars])))
    assert(fingerPrint("/avro/sets/SetOfChars.avsc") === fingerPrint(Schema[SetOfChars]))
  }

  test("Generate schema from a case class with a set of byte field") {
    assert(fingerPrint("/avro/sets/SetOfBytes.avsc") === fingerPrint(SetOfBytes(Set(1.toByte)).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfBytes.avsc") === fingerPrint(Schema(SetOfBytes(Set(1.toByte)))))
    assert(fingerPrint("/avro/sets/SetOfBytes.avsc") === fingerPrint(Schema(classOf[SetOfBytes])))
    assert(fingerPrint("/avro/sets/SetOfBytes.avsc") === fingerPrint(Schema[SetOfBytes]))
  }

  test("Generate schema from a case class with a set of short field") {
    assert(fingerPrint("/avro/sets/SetOfShorts.avsc") === fingerPrint(SetOfShorts(Set(1.toShort)).pickle.value))
    assert(fingerPrint("/avro/sets/SetOfShorts.avsc") === fingerPrint(Schema(SetOfShorts(Set(1.toShort)))))
    assert(fingerPrint("/avro/sets/SetOfShorts.avsc") === fingerPrint(Schema(classOf[SetOfShorts])))
    assert(fingerPrint("/avro/sets/SetOfShorts.avsc") === fingerPrint(Schema[SetOfShorts]))
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
