package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import scala.collection.JavaConversions._
import java.util.{List => JList}
import com.gilt.pickling.avro.AvroPicklingArrayOfPrimitivesTest._
import java.nio.ByteBuffer
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalacheck.{Gen, Arbitrary}

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


  implicit val arbArrayOfInts = Arbitrary(for (nums <- Gen.containerOf[Array, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield ArrayOfInts(nums))
  implicit val arbArrayOfLongs = Arbitrary(for (nums <- Gen.containerOf[Array, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield ArrayOfLongs(nums))
  implicit val arbArrayOfDoubles = Arbitrary(for (nums <- Gen.containerOf[Array, Double](Gen.choose(Double.MinValue / 2, Int.MaxValue / 2))) yield ArrayOfDoubles(nums))
  implicit val arbArrayOfFloats = Arbitrary(for (nums <- Gen.containerOf[Array, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield ArrayOfFloats(nums))
  implicit val arbArrayOfBooleans = Arbitrary(for (nums <- Gen.containerOf[Array, Boolean](Gen.oneOf(true, false))) yield ArrayOfBooleans(nums))
  implicit val arbArrayOfStrings = Arbitrary(for (nums <- Gen.containerOf[Array, String](Gen.alphaStr)) yield ArrayOfStrings(nums))
  implicit val arbArrayOfBytes = Arbitrary(for (nums <- Gen.containerOf[Array, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield ArrayOfBytes(nums))
  implicit val arbArrayOfShorts = Arbitrary(for (nums <- Gen.containerOf[Array, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield ArrayOfShorts(nums))
  implicit val arbArrayOfChars = Arbitrary(for (nums <- Gen.containerOf[Array, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield ArrayOfChars(nums))
}

class AvroPicklingArrayOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  // Array of Ints
  test("Pickle a case class with an array of ints") {
    forAll {
      (obj: ArrayOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfInts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of ints") {
    forAll {
      (obj: ArrayOfInts) =>
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfInts.avsc")
        val hydratedObj: ArrayOfInts = bytes.unpickle[ArrayOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of ints") {
    forAll {
      (obj: ArrayOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfInts = pckl.unpickle[ArrayOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Longs
  test("Pickle a case class with an array of longs") {
    forAll {
      (obj: ArrayOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of longs") {
    forAll {
      (obj: ArrayOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfLongs.avsc")
        val hydratedObj: ArrayOfLongs = bytes.unpickle[ArrayOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of longs") {
    forAll {
      (obj: ArrayOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfLongs = pckl.unpickle[ArrayOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Doubles
  test("Pickle a case class with an array of doubles") {
    forAll {
      (obj: ArrayOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of doubles") {
    forAll {
      (obj: ArrayOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfDoubles.avsc")
        val hydratedObj: ArrayOfDoubles = bytes.unpickle[ArrayOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of doubles") {
    forAll {
      (obj: ArrayOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfDoubles = pckl.unpickle[ArrayOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an array of floats") {
    forAll {
      (obj: ArrayOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of floats") {
    forAll {
      (obj: ArrayOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfFloats.avsc")
        val hydratedObj: ArrayOfFloats = bytes.unpickle[ArrayOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of floats") {
    forAll {
      (obj: ArrayOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfFloats = pckl.unpickle[ArrayOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an array of boolean") {
    forAll {
      (obj: ArrayOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of boolean") {
    forAll {
      (obj: ArrayOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfBooleans.avsc")
        val hydratedObj: ArrayOfBooleans = bytes.unpickle[ArrayOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of boolean") {
    forAll {
      (obj: ArrayOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfBooleans = pckl.unpickle[ArrayOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Strings
  test("Pickle a case class with an array of string") {
    forAll {
      (obj: ArrayOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of string") {
    forAll {
      (obj: ArrayOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list.toList, "/avro/array/ArrayOfStrings.avsc")
        val hydratedObj: ArrayOfStrings = bytes.unpickle[ArrayOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of string") {
    forAll {
      (obj: ArrayOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfStrings = pckl.unpickle[ArrayOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Bytes
  test("Pickle a case class with an array of bytes") {
    forAll {
      (obj: ArrayOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(ByteBuffer.wrap(obj.list), "/avro/array/ArrayOfBytes.avsc") === pckl.value)

    }
  }

  test("Unpickle a case class with an array of bytes") {
    forAll {
      (obj: ArrayOfBytes) =>
        val bytes = generateBytesFromAvro(ByteBuffer.wrap(obj.list), "/avro/array/ArrayOfBytes.avsc")
        val hydratedObj: ArrayOfBytes = bytes.unpickle[ArrayOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of bytes") {
    forAll {
      (obj: ArrayOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfBytes = pckl.unpickle[ArrayOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Shorts
  test("Pickle a case class with an array of shorts") {
    forAll {
      (obj: ArrayOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of shorts") {
    forAll {
      (obj: ArrayOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfShorts.avsc")
        val hydratedObj: ArrayOfShorts = bytes.unpickle[ArrayOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of shorts") {
    forAll {
      (obj: ArrayOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: ArrayOfShorts = pckl.unpickle[ArrayOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Chars
  test("Pickle a case class with an array of chars") {
    forAll {
      (obj: ArrayOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an array of chars") {
    forAll {
      (obj: ArrayOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.toList.map(_.toInt), "/avro/array/ArrayOfChars.avsc")
        val hydratedObj: ArrayOfChars = bytes.unpickle[ArrayOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an array of chars") {
    forAll {
      (obj: ArrayOfChars) =>
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
