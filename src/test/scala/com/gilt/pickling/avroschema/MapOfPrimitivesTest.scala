package com.gilt.pickling.avroschema

import org.scalatest.{Assertions, FunSuite}
import com.gilt.pickling.TestObjs._
import scala.pickling._
import com.gilt.pickling.TestUtils._

class MapOfPrimitivesTest extends FunSuite with Assertions {

  test("Generate schema from a case class with a map of int field") {
    assert(fingerPrint("/avro/maps/MapOfInts.avsc") === fingerPrint(MapOfInts(Map("key" -> 123)).pickle.value))
  }

  test("Generate schema from a case class with a map of long field") {
    assert(fingerPrint("/avro/maps/MapOfLongs.avsc") === fingerPrint(MapOfLongs(Map("key" ->123L)).pickle.value))
  }

  test("Generate schema from a case class with a map of double field") {
    assert(fingerPrint("/avro/maps/MapOfDoubles.avsc") === fingerPrint(MapOfDoubles(Map("key" ->123D)).pickle.value))
  }

  test("Generate schema from a case class with a map of float field") {
    assert(fingerPrint("/avro/maps/MapOfFloats.avsc") === fingerPrint(MapOfFloats(Map("key" ->123F)).pickle.value))
  }

  test("Generate schema from a case class with a map of boolean field") {
    assert(fingerPrint("/avro/maps/MapOfBooleans.avsc") === fingerPrint(MapOfBooleans(Map("key" -> false)).pickle.value))
  }

  test("Generate schema from a case class with a map of string field") {
    assert(fingerPrint("/avro/maps/MapOfStrings.avsc") === fingerPrint(MapOfStrings(Map("key" -> "abc")).pickle.value))
  }

  test("Generate schema from a case class with a map of char field") {
    assert(fingerPrint("/avro/maps/MapOfChars.avsc") === fingerPrint(MapOfChars(Map("key" -> 'a')).pickle.value))
  }

  test("Generate schema from a case class with a map of byte field") {
    assert(fingerPrint("/avro/maps/MapOfBytes.avsc") === fingerPrint(MapOfBytes(Map("key" -> 1.toByte)).pickle.value))
  }

  test("Generate schema from a case class with a map of short field") {
    assert(fingerPrint("/avro/maps/MapOfShorts.avsc") === fingerPrint(MapOfShorts(Map("key" -> 1.toShort)).pickle.value))
  }

  test("Generate schema from a case class with an empty map of int field") {
    assert(fingerPrint("/avro/maps/MapOfInts.avsc") === fingerPrint(MapOfInts(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of long field") {
    assert(fingerPrint("/avro/maps/MapOfLongs.avsc") === fingerPrint(MapOfLongs(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of double field") {
    assert(fingerPrint("/avro/maps/MapOfDoubles.avsc") === fingerPrint(MapOfDoubles(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of float field") {
    assert(fingerPrint("/avro/maps/MapOfFloats.avsc") === fingerPrint(MapOfFloats(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of boolean field") {
    assert(fingerPrint("/avro/maps/MapOfBooleans.avsc") === fingerPrint(MapOfBooleans(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of string field") {
    assert(fingerPrint("/avro/maps/MapOfStrings.avsc") === fingerPrint(MapOfStrings(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of char field") {
    assert(fingerPrint("/avro/maps/MapOfChars.avsc") === fingerPrint(MapOfChars(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of byte field") {
    assert(fingerPrint("/avro/maps/MapOfBytes.avsc") === fingerPrint(MapOfBytes(Map()).pickle.value))
  }

  test("Generate schema from a case class with an empty map of short field") {
    assert(fingerPrint("/avro/maps/MapOfShorts.avsc") === fingerPrint(MapOfShorts(Map()).pickle.value))
  }

}
