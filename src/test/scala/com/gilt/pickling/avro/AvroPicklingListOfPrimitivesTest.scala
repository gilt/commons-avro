package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import avro._
import scala.collection.JavaConversions._
import java.util.{List => JList}
import AvroPicklingListOfPrimitivesTest._

object AvroPicklingListOfPrimitivesTest {
  case class ListOfInts(list: List[Int])
  case class ListOfLongs(list: List[Long])
  case class ListOfDoubles(list: List[Double])
  case class ListOfFloats(list: List[Float])
  case class ListOfBooleans(list: List[Boolean])
  case class ListOfStrings(list: List[String])
  case class ListOfBytes(list: List[Byte])
  case class ListOfShorts(list: List[Short])
  case class ListOfChars(list: List[Char])
}

class AvroPicklingListOfPrimitivesTest extends FunSuite with Assertions {

  // Array of Ints
  test("Pickle a case class with an array of ints") {
    val obj = ListOfInts(List(1, 2, 3, 4))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfInts.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of ints") {
    val obj = ListOfInts(List(1, 2, 3, 4))
    val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfInts.avsc")
    val hydratedObj: ListOfInts = bytes.unpickle[ListOfInts]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of ints") {
    import binary._
    val obj = ListOfInts(List(1, 2, 3, 4))
    val pckl = obj.pickle
    val hydratedObj: ListOfInts = pckl.unpickle[ListOfInts]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Longs
  test("Pickle a case class with an array of longs") {
    val obj = ListOfLongs(List(1L, 2L, 3L, 4L))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfLongs.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of longs") {
    val obj = ListOfLongs(List(1L, 2L, 3L, 4L))
    val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfLongs.avsc")
    val hydratedObj: ListOfLongs = bytes.unpickle[ListOfLongs]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of longs") {
    val obj = ListOfLongs(List(1L, 2L, 3L, 4L))
    val pckl = obj.pickle
    val hydratedObj: ListOfLongs = pckl.unpickle[ListOfLongs]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Doubles
  test("Pickle a case class with an array of doubles") {
    val obj = ListOfDoubles(List(1.0, 2.0, 3.0, 4.0))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfDoubles.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of doubles") {
    val obj = ListOfDoubles(List(1.0, 2.0, 3.0, 4.0))
    val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfDoubles.avsc")
    val hydratedObj: ListOfDoubles = bytes.unpickle[ListOfDoubles]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of doubles") {
    val obj = ListOfDoubles(List(1.0, 2.0, 3.0, 4.0))
    val pckl = obj.pickle
    val hydratedObj: ListOfDoubles = pckl.unpickle[ListOfDoubles]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Floats
  test("Pickle a case class with an array of floats") {
    val obj = ListOfFloats(List(1.1F, 2.2F, 3.3F, 4.4F))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfFloats.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of floats") {
    val obj = ListOfFloats(List(1.1F, 2.2F, 3.3F, 4.4F))
    val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfFloats.avsc")
    val hydratedObj: ListOfFloats = bytes.unpickle[ListOfFloats]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of floats") {
    val obj = ListOfFloats(List(1.1F, 2.2F, 3.3F, 4.4F))
    val pckl = obj.pickle
    val hydratedObj: ListOfFloats = pckl.unpickle[ListOfFloats]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Floats
  test("Pickle a case class with an array of boolean") {
    val obj = ListOfBooleans(List(true, false, true, true))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfBooleans.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of boolean") {
    val obj = ListOfBooleans(List(true, false, true, true))
    val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfBooleans.avsc")
    val hydratedObj: ListOfBooleans = bytes.unpickle[ListOfBooleans]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of boolean") {
    val obj = ListOfBooleans(List(true, false, true, true))
    val pckl = obj.pickle
    val hydratedObj: ListOfBooleans = pckl.unpickle[ListOfBooleans]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Strings
  test("Pickle a case class with an array of string") {
    val obj = ListOfStrings(List[String]("a", "b", "c", "d"))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfStrings.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of string") {
    val obj = ListOfStrings(List[String]("a", "b", "c", "d"))
    val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfStrings.avsc")
    val hydratedObj: ListOfStrings = bytes.unpickle[ListOfStrings]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of string") {
    val obj = ListOfStrings(List[String]("a", "b", "c", "d"))
    val pckl = obj.pickle
    val hydratedObj: ListOfStrings = pckl.unpickle[ListOfStrings]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Bytes
  // TODO A better solution is to write this a as bytebuffer
  test("Pickle a case class with an array of bytes") {
    val obj = ListOfBytes(List(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfBytes.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of bytes") {
    val obj = ListOfBytes(List(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfBytes.avsc")
    val hydratedObj: ListOfBytes = bytes.unpickle[ListOfBytes]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of bytes") {
    val obj = ListOfBytes(List(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val pckl = obj.pickle
    val hydratedObj: ListOfBytes = pckl.unpickle[ListOfBytes]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Shorts
  test("Pickle a case class with an array of shorts") {
    val obj = ListOfShorts(List(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfShorts.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of shorts") {
    val obj = ListOfShorts(List(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfShorts.avsc")
    val hydratedObj: ListOfShorts = bytes.unpickle[ListOfShorts]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of shorts") {
    val obj = ListOfShorts(List(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val pckl = obj.pickle
    val hydratedObj: ListOfShorts = pckl.unpickle[ListOfShorts]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Chars
  test("Pickle a case class with an array of chars") {
    val obj = ListOfChars(List(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfChars.avsc") === pckl.value)
  }

  test("Unpickle a case class with an array of chars") {
    val obj = ListOfChars(List(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfChars.avsc")
    val hydratedObj: ListOfChars = bytes.unpickle[ListOfChars]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an array of chars") {
    val obj = ListOfChars(List(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val pckl = obj.pickle
    val hydratedObj: ListOfChars = pckl.unpickle[ListOfChars]
    assert(obj.list === hydratedObj.list)
  }

  private def generateBytesFromAvro(value: JList[Any], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }
}
