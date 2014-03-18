package com.gilt.pickling.avro

import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import scala.List
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils
import TestUtils._
import scala.Some
import com.gilt.pickling.avro.OptionListOfPrimitivesTest._
import org.apache.avro.generic.GenericData
import scala.pickling._
import scala.collection.JavaConverters._
import com.gilt.pickling.TestObjs._

object OptionListOfPrimitivesTest {
  def opt[T](exists: Boolean, value: T) = if (exists) Some(value) else None

  implicit val arbOptionListOfInts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield OpListOfInts(opt(exists, nums)))
  implicit val arbOptionListOfLongs = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield OpListOfLongs(opt(exists, nums)))
  implicit val arbOptionListOfDoubles = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Double](Gen.choose(Double.MinValue / 2, Int.MaxValue / 2))) yield OpListOfDoubles(opt(exists, nums)))
  implicit val arbOptionListOfFloats = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield OpListOfFloats(opt(exists, nums)))
  implicit val arbOptionListOfBooleans = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Boolean](Gen.oneOf(true, false))) yield OpListOfBooleans(opt(exists, nums)))
  implicit val arbOptionListOfStrings = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, String](Gen.alphaStr)) yield OpListOfStrings(opt(exists, nums)))
  implicit val arbOptionListOfBytes = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield OpListOfBytes(opt(exists, nums)))
  implicit val arbOptionListOfShorts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield OpListOfShorts(opt(exists, nums)))
  implicit val arbOptionListOfChars = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield OpListOfChars(opt(exists, nums)))

}

class OptionListOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks{

  // List of Ints
  test("Pickle a case class with an optional list of ints") {
    forAll {
      (obj: OpListOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfInts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of ints") {
    forAll {
      (obj: OpListOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfInts.avsc")
        val hydratedObj: OpListOfInts = bytes.unpickle[OpListOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of ints") {
    forAll {
      (obj: OpListOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfInts = pckl.unpickle[OpListOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Longs
  test("Pickle a case class with an optional list of Longs") {
    forAll {
      (obj: OpListOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Longs") {
    forAll {
      (obj: OpListOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfLongs.avsc")
        val hydratedObj: OpListOfLongs = bytes.unpickle[OpListOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Longs") {
    forAll {
      (obj: OpListOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfLongs = pckl.unpickle[OpListOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Doubles
  test("Pickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OpListOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OpListOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfDoubles.avsc")
        val hydratedObj: OpListOfDoubles = bytes.unpickle[OpListOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Doubles") {
    forAll {
      (obj: OpListOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfDoubles = pckl.unpickle[OpListOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Floats
  test("Pickle a case class with an optional list of Floats") {
    forAll {
      (obj: OpListOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Floats") {
    forAll {
      (obj: OpListOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfFloats.avsc")
        val hydratedObj: OpListOfFloats = bytes.unpickle[OpListOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Floats") {
    forAll {
      (obj: OpListOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfFloats = pckl.unpickle[OpListOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Booleans
  test("Pickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OpListOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OpListOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfBooleans.avsc")
        val hydratedObj: OpListOfBooleans = bytes.unpickle[OpListOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Booleans") {
    forAll {
      (obj: OpListOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfBooleans = pckl.unpickle[OpListOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Strings
  test("Pickle a case class with an optional list of Strings") {
    forAll {
      (obj: OpListOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Strings") {
    forAll {
      (obj: OpListOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfStrings.avsc")
        val hydratedObj: OpListOfStrings = bytes.unpickle[OpListOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Strings") {
    forAll {
      (obj: OpListOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfStrings = pckl.unpickle[OpListOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Bytes
  test("Pickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OpListOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OpListOfBytes) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfBytes.avsc")
        val hydratedObj: OpListOfBytes = bytes.unpickle[OpListOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Bytes") {
    forAll {
      (obj: OpListOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfBytes = pckl.unpickle[OpListOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Chars
  test("Pickle a case class with an optional list of Chars") {
    forAll {
      (obj: OpListOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Chars") {
    forAll {
      (obj: OpListOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfChars.avsc")
        val hydratedObj: OpListOfChars = bytes.unpickle[OpListOfChars]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Chars") {
    forAll {
      (obj: OpListOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfChars = pckl.unpickle[OpListOfChars]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Shorts
  test("Pickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OpListOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OpListOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfShorts.avsc")
        val hydratedObj: OpListOfShorts = bytes.unpickle[OpListOfShorts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Shorts") {
    forAll {
      (obj: OpListOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: OpListOfShorts = pckl.unpickle[OpListOfShorts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  private def generateBytesFromAvro(value: Option[List[_]], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) => record.put("list", x.asJavaCollection)
      case _ => record.put("list", null)
    }
    convertToBytes(schema, record)
  }

}
