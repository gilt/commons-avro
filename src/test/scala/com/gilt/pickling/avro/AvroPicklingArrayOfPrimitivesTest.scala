package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import avro._
import scala.collection.JavaConversions._
import java.util.{List => JList}
import com.gilt.pickling.avro.AvroPicklingArrayOfPrimitivesTest._
import java.nio.ByteBuffer

object AvroPicklingArrayOfPrimitivesTest {

  case class ArrayOfInts(list: Array[Int])
  case class ArrayOfLongs(list: Array[Long])
  case class ArrayOfDoubles(list: Array[Double])
  case class ArrayOfFloats(list: Array[Float])
  case class ArrayOfBooleans(list: Array[Boolean])
  case class ArrayOfStrings(list: Array[String])
  case class ArrayOfBytes(list: Array[Byte])
  case class ArrayOfShorts(list: Array[Short])
  case class ArrayOfChars(list: Array[Char])
}

class AvroPicklingArrayOfPrimitivesTest extends FunSuite with Assertions {

  // Array of Ints
  test("Pickle a case class with an array of ints") {
    val obj = ArrayOfInts(Array(1, 2, 3, 4))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfInts.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of ints") {
    val obj = ArrayOfInts(Array(1, 2, 3, 4))
    val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfInts.avsc")
    val hydratedObj: ArrayOfInts = bytes.unpickle[ArrayOfInts]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of ints") {
    val obj = ArrayOfInts(Array(1, 2, 3, 4))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfInts = pckl.unpickle[ArrayOfInts]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Longs
  test("Pickle a case class with an array of longs") {
    val obj = ArrayOfLongs(Array(1L, 2L, 3L, 4L))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfLongs.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of longs") {
    val obj = ArrayOfLongs(Array(1L, 2L, 3L, 4L))
    val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfLongs.avsc")
    val hydratedObj: ArrayOfLongs = bytes.unpickle[ArrayOfLongs]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of longs") {
    val obj = ArrayOfLongs(Array(1L, 2L, 3L, 4L))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfLongs = pckl.unpickle[ArrayOfLongs]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Doubles
  test("Pickle a case class with an array of doubles") {
    val obj = ArrayOfDoubles(Array(1.0, 2.0, 3.0, 4.0))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfDoubles.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of doubles") {
    val obj = ArrayOfDoubles(Array(1.0, 2.0, 3.0, 4.0))
    val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfDoubles.avsc")
    val hydratedObj: ArrayOfDoubles = bytes.unpickle[ArrayOfDoubles]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of doubles") {
    val obj = ArrayOfDoubles(Array(1.0, 2.0, 3.0, 4.0))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfDoubles = pckl.unpickle[ArrayOfDoubles]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Floats
  test("Pickle a case class with an array of floats") {
    val obj = ArrayOfFloats(Array(1.1F, 2.2F, 3.3F, 4.4F))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfFloats.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of floats") {
    val obj = ArrayOfFloats(Array(1.1F, 2.2F, 3.3F, 4.4F))
    val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfFloats.avsc")
    val hydratedObj: ArrayOfFloats = bytes.unpickle[ArrayOfFloats]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of floats") {
    val obj = ArrayOfFloats(Array(1.1F, 2.2F, 3.3F, 4.4F))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfFloats = pckl.unpickle[ArrayOfFloats]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Floats
  test("Pickle a case class with an array of boolean") {
    val obj = ArrayOfBooleans(Array(true, false, true, true))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfBooleans.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of boolean") {
    val obj = ArrayOfBooleans(Array(true, false, true, true))
    val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfBooleans.avsc")
    val hydratedObj: ArrayOfBooleans = bytes.unpickle[ArrayOfBooleans]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of boolean") {
    val obj = ArrayOfBooleans(Array(true, false, true, true))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfBooleans = pckl.unpickle[ArrayOfBooleans]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Strings
  test("Pickle a case class with an array of string") {
    val obj = ArrayOfStrings(Array[String]("a", "b", "c", "d"))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfStrings.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of string") {
    val obj = ArrayOfStrings(Array[String]("a", "b", "c", "d"))
    val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfStrings.avsc")
    val hydratedObj: ArrayOfStrings = bytes.unpickle[ArrayOfStrings]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of string") {
    val obj = ArrayOfStrings(Array[String]("a", "b", "c", "d"))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfStrings = pckl.unpickle[ArrayOfStrings]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Bytes
  test("Pickle a case class with an array of bytes") {
    val obj = ArrayOfBytes(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(ByteBuffer.wrap(obj.list), "/avro/array/ArrayOfBytes.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of bytes") {
    val obj = ArrayOfBytes(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val bytes = generateBytesFromAvro(ByteBuffer.wrap(obj.list), "/avro/array/ArrayOfBytes.avsc")
    val hydratedObj: ArrayOfBytes = bytes.unpickle[ArrayOfBytes]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of bytes") {
    val obj = ArrayOfBytes(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfBytes = pckl.unpickle[ArrayOfBytes]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Shorts
  test("Pickle a case class with an array of shorts") {
    val obj = ArrayOfShorts(Array(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfShorts.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of shorts") {
    val obj = ArrayOfShorts(Array(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val bytes = generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfShorts.avsc")
    val hydratedObj: ArrayOfShorts = bytes.unpickle[ArrayOfShorts]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of shorts") {
    val obj = ArrayOfShorts(Array(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfShorts = pckl.unpickle[ArrayOfShorts]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Chars
  test("Pickle a case class with an array of chars") {
    val obj = ArrayOfChars(Array(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfChars.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of chars") {
    val obj = ArrayOfChars(Array(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val bytes = generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfChars.avsc")
    val hydratedObj: ArrayOfChars = bytes.unpickle[ArrayOfChars]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of chars") {
    val obj = ArrayOfChars(Array(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val pckl = obj.pickle
    val hydratedObj: ArrayOfChars = pckl.unpickle[ArrayOfChars]
    assert(obj.list === hydratedObj.list)
  }

  private def generateBytesFromAvro(value: JList[Any], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }

  private def generateBytesFromAvro(value: ByteBuffer, schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }
}
