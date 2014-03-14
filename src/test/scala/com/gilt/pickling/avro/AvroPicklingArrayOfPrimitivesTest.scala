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
import org.scalatest.prop.GeneratorDrivenPropertyChecks

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

class AvroPicklingArrayOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  // Array of Ints
  test("Pickle a case class with an array of ints") {
    forAll {
      (ints: Array[Int]) =>
        val obj = ArrayOfInts(ints)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfInts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of ints") {
    forAll {
      (ints: Array[Int]) =>
        val obj = ArrayOfInts(ints)
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfInts.avsc")
        val hydratedObj: ArrayOfInts = bytes.unpickle[ArrayOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of ints") {
    forAll {
      (ints: Array[Int]) =>
        val obj = ArrayOfInts(ints)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfInts = pckl.unpickle[ArrayOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Longs
  test("Pickle a case class with an array of longs") {
    forAll {
      (longs: Array[Long]) =>
        val obj = ArrayOfLongs(longs)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of longs") {
    forAll {
      (longs: Array[Long]) =>
        val obj = ArrayOfLongs(longs)
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfLongs.avsc")
        val hydratedObj: ArrayOfLongs = bytes.unpickle[ArrayOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of longs") {
    forAll {
      (longs: Array[Long]) =>
        val obj = ArrayOfLongs(longs)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfLongs = pckl.unpickle[ArrayOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Doubles
  test("Pickle a case class with an array of doubles") {
    forAll {
      (doubles: Array[Double]) =>
        val obj = ArrayOfDoubles(doubles)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of doubles") {
    forAll {
      (doubles: Array[Double]) =>
        val obj = ArrayOfDoubles(doubles)
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfDoubles.avsc")
        val hydratedObj: ArrayOfDoubles = bytes.unpickle[ArrayOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of doubles") {
    forAll {
      (doubles: Array[Double]) =>
        val obj = ArrayOfDoubles(doubles)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfDoubles = pckl.unpickle[ArrayOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an array of floats") {
    forAll {
      (floats: Array[Float]) =>
        val obj = ArrayOfFloats(floats)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of floats") {
    forAll {
      (floats: Array[Float]) =>
        val obj = ArrayOfFloats(floats)
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfFloats.avsc")
        val hydratedObj: ArrayOfFloats = bytes.unpickle[ArrayOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of floats") {
    forAll {
      (floats: Array[Float]) =>
        val obj = ArrayOfFloats(floats)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfFloats = pckl.unpickle[ArrayOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an array of boolean") {
    forAll {
      (booleans: Array[Boolean]) =>
        val obj = ArrayOfBooleans(booleans)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of boolean") {
    forAll {
      (booleans: Array[Boolean]) =>
        val obj = ArrayOfBooleans(booleans)
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfBooleans.avsc")
        val hydratedObj: ArrayOfBooleans = bytes.unpickle[ArrayOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of boolean") {
    forAll {
      (booleans: Array[Boolean]) =>
        val obj = ArrayOfBooleans(booleans)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfBooleans = pckl.unpickle[ArrayOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Strings
  test("Pickle a case class with an array of string") {
    forAll {
      (strings: Array[String]) =>
        val obj = ArrayOfStrings(strings)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of string") {
    forAll {
      (strings: Array[String]) =>
        val obj = ArrayOfStrings(strings)
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfStrings.avsc")
        val hydratedObj: ArrayOfStrings = bytes.unpickle[ArrayOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of string") {
    forAll {
      (strings: Array[String]) =>
        val obj = ArrayOfStrings(strings)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfStrings = pckl.unpickle[ArrayOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Bytes
  test("Pickle a case class with an array of bytes") {
    forAll {
      (bytes: Array[Byte]) =>
        val obj = ArrayOfBytes(bytes)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(ByteBuffer.wrap(obj.list), "/avro/array/ArrayOfBytes.avsc") === pckl.value)

    }
  }

  test("Unpickle a case class with an array of bytes") {
    forAll {
      (b: Array[Byte]) =>
        val obj = ArrayOfBytes(b)
        val bytes = generateBytesFromAvro(ByteBuffer.wrap(obj.list), "/avro/array/ArrayOfBytes.avsc")
        val hydratedObj: ArrayOfBytes = bytes.unpickle[ArrayOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of bytes") {
    forAll {
      (b: Array[Byte]) =>
        val obj = ArrayOfBytes(b)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfBytes = pckl.unpickle[ArrayOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Shorts
  test("Pickle a case class with an array of shorts") {
    forAll {
      (shorts: Array[Short]) =>
        val obj = ArrayOfShorts(shorts)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of shorts") {
    forAll {
      (shorts: Array[Short]) =>
        val obj = ArrayOfShorts(shorts)
        val bytes = generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfShorts.avsc")
        val hydratedObj: ArrayOfShorts = bytes.unpickle[ArrayOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of shorts") {
    forAll {
      (shorts: Array[Short]) =>
        val obj = ArrayOfShorts(shorts)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfShorts = pckl.unpickle[ArrayOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Chars
  test("Pickle a case class with an array of chars") {
    forAll {
      (chars: Array[Char]) =>
        val obj = ArrayOfChars(chars)
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of chars") {
    forAll {
      (chars: Array[Char]) =>
        val obj = ArrayOfChars(chars)
        val bytes = generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfChars.avsc")
        val hydratedObj: ArrayOfChars = bytes.unpickle[ArrayOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of chars") {
    forAll {
      (chars: Array[Char]) =>
        val obj = ArrayOfChars(chars)
        val pckl = obj.pickle
        val hydratedObj: ArrayOfChars = pckl.unpickle[ArrayOfChars]
        assert(obj.list === hydratedObj.list)
    }
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
