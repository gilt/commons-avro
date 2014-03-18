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

  implicit val arbOptionArrayOfInts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield OpArrayOfInts(opt(exists, nums)))
  implicit val arbOptionArrayOfLongs = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield OpArrayOfLongs(opt(exists, nums)))
  implicit val arbOptionArrayOfDoubles = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Double](Gen.choose(Double.MinValue / 2, Int.MaxValue / 2))) yield OpArrayOfDoubles(opt(exists, nums)))
  implicit val arbOptionArrayOfFloats = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield OpArrayOfFloats(opt(exists, nums)))
  implicit val arbOptionArrayOfBooleans = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Boolean](Gen.oneOf(true, false))) yield OpArrayOfBooleans(opt(exists, nums)))
  implicit val arbOptionArrayOfStrings = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, String](Gen.alphaStr)) yield OpArrayOfStrings(opt(exists, nums)))
  implicit val arbOptionArrayOfBytes = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield OpArrayOfBytes(opt(exists, nums)))
  implicit val arbOptionArrayOfShorts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield OpArrayOfShorts(opt(exists, nums)))
  implicit val arbOptionArrayOfChars = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Array, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield OpArrayOfChars(opt(exists, nums)))
}

class OptionArrayOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  // Array of Ints
  test("Pickle a case class with an optional array of ints") {
    forAll {
      (obj: OpArrayOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfInts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of ints") {
    forAll {
      (obj: OpArrayOfInts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfInts.avsc")
        val hydratedObj: OpArrayOfInts = bytes.unpickle[OpArrayOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of ints") {
    forAll {
      (obj: OpArrayOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfInts = pckl.unpickle[OpArrayOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Longs
  test("Pickle a case class with an optional array of longs") {
    forAll {
      (obj: OpArrayOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of longs") {
    forAll {
      (obj: OpArrayOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfLongs.avsc")
        val hydratedObj: OpArrayOfLongs = bytes.unpickle[OpArrayOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of longs") {
    forAll {
      (obj: OpArrayOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfLongs = pckl.unpickle[OpArrayOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Doubles
  test("Pickle a case class with an optional array of Doubles") {
    forAll {
      (obj: OpArrayOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Doubles") {
    forAll {
      (obj: OpArrayOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfDoubles.avsc")
        val hydratedObj: OpArrayOfDoubles = bytes.unpickle[OpArrayOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Doubles") {
    forAll {
      (obj: OpArrayOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfDoubles = pckl.unpickle[OpArrayOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Floats
  test("Pickle a case class with an optional array of Floats") {
    forAll {
      (obj: OpArrayOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Floats") {
    forAll {
      (obj: OpArrayOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfFloats.avsc")
        val hydratedObj: OpArrayOfFloats = bytes.unpickle[OpArrayOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Floats") {
    forAll {
      (obj: OpArrayOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfFloats = pckl.unpickle[OpArrayOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Booleans
  test("Pickle a case class with an optional array of Booleans") {
    forAll {
      (obj: OpArrayOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Booleans") {
    forAll {
      (obj: OpArrayOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfBooleans.avsc")
        val hydratedObj: OpArrayOfBooleans = bytes.unpickle[OpArrayOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Booleans") {
    forAll {
      (obj: OpArrayOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfBooleans = pckl.unpickle[OpArrayOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Strings
  test("Pickle a case class with an optional array of Strings") {
    forAll {
      (obj: OpArrayOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Strings") {
    forAll {
      (obj: OpArrayOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList), "/avro/option-array/OptionArrayOfStrings.avsc")
        val hydratedObj: OpArrayOfStrings = bytes.unpickle[OpArrayOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Strings") {
    forAll {
      (obj: OpArrayOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfStrings = pckl.unpickle[OpArrayOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Bytes
  test("Pickle a case class with an optional array of Bytes") {
    forAll {
      (obj: OpArrayOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvroWithByteBuffer(obj.list.map(ByteBuffer.wrap(_)), "/avro/option-array/OptionArrayOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Bytes") {
    forAll {
      (obj: OpArrayOfBytes) =>
        val bytes = generateBytesFromAvroWithByteBuffer(obj.list.map(ByteBuffer.wrap(_)), "/avro/option-array/OptionArrayOfBytes.avsc")
        val hydratedObj: OpArrayOfBytes = bytes.unpickle[OpArrayOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Bytes") {
    forAll {
      (obj: OpArrayOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfBytes = pckl.unpickle[OpArrayOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Shorts
  test("Pickle a case class with an optional array of Shorts") {
    forAll {
      (obj: OpArrayOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Shorts") {
    forAll {
      (obj: OpArrayOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfShorts.avsc")
        val hydratedObj: OpArrayOfShorts = bytes.unpickle[OpArrayOfShorts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Shorts") {
    forAll {
      (obj: OpArrayOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfShorts = pckl.unpickle[OpArrayOfShorts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // Array of Chars
  test("Pickle a case class with an optional array of Chars") {
    forAll {
      (obj: OpArrayOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional array of Chars") {
    forAll {
      (obj: OpArrayOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toList.map(_.toInt)), "/avro/option-array/OptionArrayOfChars.avsc")
        val hydratedObj: OpArrayOfChars = bytes.unpickle[OpArrayOfChars]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional array of Chars") {
    forAll {
      (obj: OpArrayOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: OpArrayOfChars = pckl.unpickle[OpArrayOfChars]
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
