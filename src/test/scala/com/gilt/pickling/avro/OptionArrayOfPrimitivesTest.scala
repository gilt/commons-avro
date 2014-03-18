package com.gilt.pickling.avro

import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils
import TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import java.util.{List => JList}
import scala.Some
import com.gilt.pickling.avro.OptionArrayOfPrimitivesTest._
import scala.collection.JavaConversions._
import java.nio.ByteBuffer
import com.gilt.pickling.TestObjs._

object OptionArrayOfPrimitivesTest {

  def opt[T](exists: Boolean, value: T) = if (exists) Some(value) else None

  implicit val arbOptionArrayOfInts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield OptionArrayOfInts(opt(exists, nums)))
  implicit val arbOptionArrayOfLongs = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield OptionArrayOfLongs(opt(exists, nums)))
  implicit val arbOptionArrayOfDoubles = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Double](Gen.choose(Double.MinValue / 2, Int.MaxValue / 2))) yield OptionArrayOfDoubles(opt(exists, nums)))
  implicit val arbOptionArrayOfFloats = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield OptionArrayOfFloats(opt(exists, nums)))
  implicit val arbOptionArrayOfBooleans = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Boolean](Gen.oneOf(true, false))) yield OptionArrayOfBooleans(opt(exists, nums)))
  implicit val arbOptionArrayOfStrings = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, String](Gen.alphaStr)) yield OptionArrayOfStrings(opt(exists, nums)))
  implicit val arbOptionArrayOfBytes = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield OptionArrayOfBytes(opt(exists, nums)))
  implicit val arbOptionArrayOfShorts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield OptionArrayOfShorts(opt(exists, nums)))
  implicit val arbOptionArrayOfChars = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield OptionArrayOfChars(opt(exists, nums)))
}

class OptionArrayOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  // Array of Ints
  test("Pickle a case class with an optional array of ints") {
    forAll {
      (obj: OptionArrayOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfInts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of ints") {
    forAll {
      (obj: OptionArrayOfInts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfInts.avsc")
        val hydratedObj: OptionArrayOfInts = bytes.unpickle[OptionArrayOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of ints") {
    forAll {
      (obj: OptionArrayOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfInts = pckl.unpickle[OptionArrayOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Longs
  test("Pickle a case class with an optional array of longs") {
    forAll {
      (obj: OptionArrayOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of longs") {
    forAll {
      (obj: OptionArrayOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfLongs.avsc")
        val hydratedObj: OptionArrayOfLongs = bytes.unpickle[OptionArrayOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of longs") {
    forAll {
      (obj: OptionArrayOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfLongs = pckl.unpickle[OptionArrayOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Doubles
  test("Pickle a case class with an optional array of Doubles") {
    forAll {
      (obj: OptionArrayOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Doubles") {
    forAll {
      (obj: OptionArrayOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfDoubles.avsc")
        val hydratedObj: OptionArrayOfDoubles = bytes.unpickle[OptionArrayOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Doubles") {
    forAll {
      (obj: OptionArrayOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfDoubles = pckl.unpickle[OptionArrayOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Floats
  test("Pickle a case class with an optional array of Floats") {
    forAll {
      (obj: OptionArrayOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Floats") {
    forAll {
      (obj: OptionArrayOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfFloats.avsc")
        val hydratedObj: OptionArrayOfFloats = bytes.unpickle[OptionArrayOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Floats") {
    forAll {
      (obj: OptionArrayOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfFloats = pckl.unpickle[OptionArrayOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Booleans
  test("Pickle a case class with an optional array of Booleans") {
    forAll {
      (obj: OptionArrayOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Booleans") {
    forAll {
      (obj: OptionArrayOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfBooleans.avsc")
        val hydratedObj: OptionArrayOfBooleans = bytes.unpickle[OptionArrayOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Booleans") {
    forAll {
      (obj: OptionArrayOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfBooleans = pckl.unpickle[OptionArrayOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Strings
  test("Pickle a case class with an optional array of Strings") {
    forAll {
      (obj: OptionArrayOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Strings") {
    forAll {
      (obj: OptionArrayOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfStrings.avsc")
        val hydratedObj: OptionArrayOfStrings = bytes.unpickle[OptionArrayOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Strings") {
    forAll {
      (obj: OptionArrayOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfStrings = pckl.unpickle[OptionArrayOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Bytes
  test("Pickle a case class with an optional array of Bytes") {
    forAll {
      (obj: OptionArrayOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvroWithByteBuffer(obj.list.map(ByteBuffer.wrap(_)), "/avro/option-array/OptionArrayOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Bytes") {
    forAll {
      (obj: OptionArrayOfBytes) =>
        val bytes = generateBytesFromAvroWithByteBuffer(obj.list.map(ByteBuffer.wrap(_)), "/avro/option-array/OptionArrayOfBytes.avsc")
        val hydratedObj: OptionArrayOfBytes = bytes.unpickle[OptionArrayOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Bytes") {
    forAll {
      (obj: OptionArrayOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfBytes = pckl.unpickle[OptionArrayOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Shorts
  test("Pickle a case class with an optional array of Shorts") {
    forAll {
      (obj: OptionArrayOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Shorts") {
    forAll {
      (obj: OptionArrayOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfShorts.avsc")
        val hydratedObj: OptionArrayOfShorts = bytes.unpickle[OptionArrayOfShorts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Shorts") {
    forAll {
      (obj: OptionArrayOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfShorts = pckl.unpickle[OptionArrayOfShorts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Chars
  test("Pickle a case class with an optional array of Chars") {
    forAll {
      (obj: OptionArrayOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Chars") {
    forAll {
      (obj: OptionArrayOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfChars.avsc")
        val hydratedObj: OptionArrayOfChars = bytes.unpickle[OptionArrayOfChars]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Chars") {
    forAll {
      (obj: OptionArrayOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: OptionArrayOfChars = pckl.unpickle[OptionArrayOfChars]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  private def generateBytesFromAvro(value: Option[JList[_]], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) => record.put("list", x)
      case _ => record.put("list", null)
    }
    convertToBytes(schema, record)
  }

  private def generateBytesFromAvroWithByteBuffer(value: Option[ByteBuffer], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) => record.put("list", x)
      case _ => record.put("list", null)
    }
    convertToBytes(schema, record)
  }

}
