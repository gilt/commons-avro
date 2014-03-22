package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class ArrayOfPrimitivesTest extends FunSuite with Assertions {

  test("Generate schema from a case class with an array of int field") {
    assert(fingerPrint("/avro/array/ArrayOfInts.avsc") === fingerPrint(ArrayOfInts(Array(123)).pickle.value))
  }

  test("Generate schema from a case class with an array of long field") {
    assert(fingerPrint("/avro/array/ArrayOfLongs.avsc") === fingerPrint(ArrayOfLongs(Array(123L)).pickle.value))
  }

  test("Generate schema from a case class with an array of double field") {
    assert(fingerPrint("/avro/array/ArrayOfDoubles.avsc") === fingerPrint(ArrayOfDoubles(Array(123D)).pickle.value))
  }

  test("Generate schema from a case class with an array of float field") {
    assert(fingerPrint("/avro/array/ArrayOfFloats.avsc") === fingerPrint(ArrayOfFloats(Array(123F)).pickle.value))
  }

  test("Generate schema from a case class with an array of boolean field") {
    assert(fingerPrint("/avro/array/ArrayOfBooleans.avsc") === fingerPrint(ArrayOfBooleans(Array(false, true)).pickle.value))
  }

  test("Generate schema from a case class with an array of string field") {
    assert(fingerPrint("/avro/array/ArrayOfStrings.avsc") === fingerPrint(ArrayOfStrings(Array("abc")).pickle.value))
  }

  test("Generate schema from a case class with an array of char field") {
    assert(fingerPrint("/avro/array/ArrayOfChars.avsc") === fingerPrint(ArrayOfChars(Array('a')).pickle.value))
  }

  test("Generate schema from a case class with an array of byte field") {
    assert(fingerPrint("/avro/array/ArrayOfBytes.avsc") === fingerPrint(ArrayOfBytes(Array(1.toByte)).pickle.value))
  }

  test("Generate schema from a case class with an array of short field") {
    assert(fingerPrint("/avro/array/ArrayOfShorts.avsc") === fingerPrint(ArrayOfShorts(Array(1.toShort)).pickle.value))
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
