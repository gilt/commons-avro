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

  implicit val arbOptionListOfInts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield OptionListOfInts(opt(exists, nums)))
  implicit val arbOptionListOfLongs = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield OptionListOfLongs(opt(exists, nums)))
  implicit val arbOptionListOfDoubles = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Double](Gen.choose(Double.MinValue / 2, Int.MaxValue / 2))) yield OptionListOfDoubles(opt(exists, nums)))
  implicit val arbOptionListOfFloats = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield OptionListOfFloats(opt(exists, nums)))
  implicit val arbOptionListOfBooleans = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Boolean](Gen.oneOf(true, false))) yield OptionListOfBooleans(opt(exists, nums)))
  implicit val arbOptionListOfStrings = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, String](Gen.alphaStr)) yield OptionListOfStrings(opt(exists, nums)))
  implicit val arbOptionListOfBytes = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield OptionListOfBytes(opt(exists, nums)))
  implicit val arbOptionListOfShorts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield OptionListOfShorts(opt(exists, nums)))
  implicit val arbOptionListOfChars = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[List, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield OptionListOfChars(opt(exists, nums)))

}

class OptionListOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks{

  // List of Ints
  test("Pickle a case class with an optional list of ints") {
    forAll {
      (obj: OptionListOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfInts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of ints") {
    forAll {
      (obj: OptionListOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfInts.avsc")
        val hydratedObj: OptionListOfInts = bytes.unpickle[OptionListOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of ints") {
    forAll {
      (obj: OptionListOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfInts = pckl.unpickle[OptionListOfInts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Longs
  test("Pickle a case class with an optional list of Longs") {
    forAll {
      (obj: OptionListOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Longs") {
    forAll {
      (obj: OptionListOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfLongs.avsc")
        val hydratedObj: OptionListOfLongs = bytes.unpickle[OptionListOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Longs") {
    forAll {
      (obj: OptionListOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfLongs = pckl.unpickle[OptionListOfLongs]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Doubles
  test("Pickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OptionListOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OptionListOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfDoubles.avsc")
        val hydratedObj: OptionListOfDoubles = bytes.unpickle[OptionListOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Doubles") {
    forAll {
      (obj: OptionListOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfDoubles = pckl.unpickle[OptionListOfDoubles]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Floats
  test("Pickle a case class with an optional list of Floats") {
    forAll {
      (obj: OptionListOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Floats") {
    forAll {
      (obj: OptionListOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfFloats.avsc")
        val hydratedObj: OptionListOfFloats = bytes.unpickle[OptionListOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Floats") {
    forAll {
      (obj: OptionListOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfFloats = pckl.unpickle[OptionListOfFloats]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Booleans
  test("Pickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OptionListOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OptionListOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfBooleans.avsc")
        val hydratedObj: OptionListOfBooleans = bytes.unpickle[OptionListOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Booleans") {
    forAll {
      (obj: OptionListOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfBooleans = pckl.unpickle[OptionListOfBooleans]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Strings
  test("Pickle a case class with an optional list of Strings") {
    forAll {
      (obj: OptionListOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Strings") {
    forAll {
      (obj: OptionListOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-lists/OptionListOfStrings.avsc")
        val hydratedObj: OptionListOfStrings = bytes.unpickle[OptionListOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Strings") {
    forAll {
      (obj: OptionListOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfStrings = pckl.unpickle[OptionListOfStrings]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Bytes
  test("Pickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OptionListOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OptionListOfBytes) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfBytes.avsc")
        val hydratedObj: OptionListOfBytes = bytes.unpickle[OptionListOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Bytes") {
    forAll {
      (obj: OptionListOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfBytes = pckl.unpickle[OptionListOfBytes]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Chars
  test("Pickle a case class with an optional list of Chars") {
    forAll {
      (obj: OptionListOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Chars") {
    forAll {
      (obj: OptionListOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfChars.avsc")
        val hydratedObj: OptionListOfChars = bytes.unpickle[OptionListOfChars]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Chars") {
    forAll {
      (obj: OptionListOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfChars = pckl.unpickle[OptionListOfChars]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  // List of Shorts
  test("Pickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OptionListOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OptionListOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-lists/OptionListOfShorts.avsc")
        val hydratedObj: OptionListOfShorts = bytes.unpickle[OptionListOfShorts]
        assert(obj.list.map(_.toList) === hydratedObj.list.map(_.toList))
    }
  }

  test("Round trip a case class with an optional list of Shorts") {
    forAll {
      (obj: OptionListOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: OptionListOfShorts = pckl.unpickle[OptionListOfShorts]
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
