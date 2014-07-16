package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class ArrayOfPrimitivesTest extends FunSuite with Assertions {

  test("Generate schema from a case class with an array of int field") {
    assert(fingerPrint("/avro/array/ArrayOfInts.avsc") === fingerPrint(ArrayOfInts(Array(123)).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfInts.avsc") === fingerPrint(Schema(ArrayOfInts(Array(123)))))
    assert(fingerPrint("/avro/array/ArrayOfInts.avsc") === fingerPrint(Schema(classOf[ArrayOfInts])))
    assert(fingerPrint("/avro/array/ArrayOfInts.avsc") === fingerPrint(Schema[ArrayOfInts]))
  }

  test("Generate schema from a case class with an array of long field") {
    assert(fingerPrint("/avro/array/ArrayOfLongs.avsc") === fingerPrint(ArrayOfLongs(Array(123L)).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfLongs.avsc") === fingerPrint(Schema(ArrayOfLongs(Array(123L)))))
    assert(fingerPrint("/avro/array/ArrayOfLongs.avsc") === fingerPrint(Schema(classOf[ArrayOfLongs])))
    assert(fingerPrint("/avro/array/ArrayOfLongs.avsc") === fingerPrint(Schema[ArrayOfLongs]))
  }

  test("Generate schema from a case class with an array of double field") {
    assert(fingerPrint("/avro/array/ArrayOfDoubles.avsc") === fingerPrint(ArrayOfDoubles(Array(123D)).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfDoubles.avsc") === fingerPrint(Schema(ArrayOfDoubles(Array(123D)))))
    assert(fingerPrint("/avro/array/ArrayOfDoubles.avsc") === fingerPrint(Schema(classOf[ArrayOfDoubles])))
    assert(fingerPrint("/avro/array/ArrayOfDoubles.avsc") === fingerPrint(Schema[ArrayOfDoubles]))
  }

  test("Generate schema from a case class with an array of float field") {
    assert(fingerPrint("/avro/array/ArrayOfFloats.avsc") === fingerPrint(ArrayOfFloats(Array(123F)).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfFloats.avsc") === fingerPrint(Schema(ArrayOfFloats(Array(123F)))))
    assert(fingerPrint("/avro/array/ArrayOfFloats.avsc") === fingerPrint(Schema(classOf[ArrayOfFloats])))
    assert(fingerPrint("/avro/array/ArrayOfFloats.avsc") === fingerPrint(Schema[ArrayOfFloats]))
  }

  test("Generate schema from a case class with an array of boolean field") {
    assert(fingerPrint("/avro/array/ArrayOfBooleans.avsc") === fingerPrint(ArrayOfBooleans(Array(false, true)).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfBooleans.avsc") === fingerPrint(Schema(ArrayOfBooleans(Array(false, true)))))
    assert(fingerPrint("/avro/array/ArrayOfBooleans.avsc") === fingerPrint(Schema(classOf[ArrayOfBooleans])))
    assert(fingerPrint("/avro/array/ArrayOfBooleans.avsc") === fingerPrint(Schema[ArrayOfBooleans]))
  }

  test("Generate schema from a case class with an array of string field") {
    assert(fingerPrint("/avro/array/ArrayOfStrings.avsc") === fingerPrint(ArrayOfStrings(Array("abc")).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfStrings.avsc") === fingerPrint(Schema(ArrayOfStrings(Array("abc")))))
    assert(fingerPrint("/avro/array/ArrayOfStrings.avsc") === fingerPrint(Schema(classOf[ArrayOfStrings])))
    assert(fingerPrint("/avro/array/ArrayOfStrings.avsc") === fingerPrint(Schema[ArrayOfStrings]))
  }

  test("Generate schema from a case class with an array of char field") {
    assert(fingerPrint("/avro/array/ArrayOfChars.avsc") === fingerPrint(ArrayOfChars(Array('a')).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfChars.avsc") === fingerPrint(Schema(ArrayOfChars(Array('a')))))
    assert(fingerPrint("/avro/array/ArrayOfChars.avsc") === fingerPrint(Schema(classOf[ArrayOfChars])))
    assert(fingerPrint("/avro/array/ArrayOfChars.avsc") === fingerPrint(Schema[ArrayOfChars]))
  }

  test("Generate schema from a case class with an array of byte field") {
    assert(fingerPrint("/avro/array/ArrayOfBytes.avsc") === fingerPrint(ArrayOfBytes(Array(1.toByte)).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfBytes.avsc") === fingerPrint(Schema(ArrayOfBytes(Array(1.toByte)))))
    assert(fingerPrint("/avro/array/ArrayOfBytes.avsc") === fingerPrint(Schema(classOf[ArrayOfBytes])))
    assert(fingerPrint("/avro/array/ArrayOfBytes.avsc") === fingerPrint(Schema[ArrayOfBytes]))
  }

  test("Generate schema from a case class with an array of short field") {
    assert(fingerPrint("/avro/array/ArrayOfShorts.avsc") === fingerPrint(ArrayOfShorts(Array(1.toShort)).pickle.value))
    assert(fingerPrint("/avro/array/ArrayOfShorts.avsc") === fingerPrint(Schema(ArrayOfShorts(Array(1.toShort)))))
    assert(fingerPrint("/avro/array/ArrayOfShorts.avsc") === fingerPrint(Schema(classOf[ArrayOfShorts])))
    assert(fingerPrint("/avro/array/ArrayOfShorts.avsc") === fingerPrint(Schema[ArrayOfShorts]))
  }

  test("Generate schema from a case class with an empty array of int field") {
    assert(fingerPrint("/avro/array/ArrayOfInts.avsc") === fingerPrint(ArrayOfInts(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of long field") {
    assert(fingerPrint("/avro/array/ArrayOfLongs.avsc") === fingerPrint(ArrayOfLongs(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of double field") {
    assert(fingerPrint("/avro/array/ArrayOfDoubles.avsc") === fingerPrint(ArrayOfDoubles(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of float field") {
    assert(fingerPrint("/avro/array/ArrayOfFloats.avsc") === fingerPrint(ArrayOfFloats(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of boolean field") {
    assert(fingerPrint("/avro/array/ArrayOfBooleans.avsc") === fingerPrint(ArrayOfBooleans(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of string field") {
    assert(fingerPrint("/avro/array/ArrayOfStrings.avsc") === fingerPrint(ArrayOfStrings(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of char field") {
    assert(fingerPrint("/avro/array/ArrayOfChars.avsc") === fingerPrint(ArrayOfChars(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of byte field") {
    assert(fingerPrint("/avro/array/ArrayOfBytes.avsc") === fingerPrint(ArrayOfBytes(Array()).pickle.value))
  }

  test("Generate schema from a case class with an empty array of short field") {
    assert(fingerPrint("/avro/array/ArrayOfShorts.avsc") === fingerPrint(ArrayOfShorts(Array()).pickle.value))
  }

}
