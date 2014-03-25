package com.gilt.pickling.avro

import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils
import TestUtils._
import com.gilt.pickling.avro.OptionSetOfPrimitivesTest._
import org.apache.avro.generic.GenericData
import scala.pickling._
import scala.collection.JavaConverters._
import com.gilt.pickling.TestObjs._

object OptionSetOfPrimitivesTest {
  def opt[T](exists: Boolean, value: T) = if (exists) Some(value) else None

  implicit val arbOptionSetOfInts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield OptionSetOfInts(opt(exists, nums)))
  implicit val arbOptionSetOfLongs = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield OptionSetOfLongs(opt(exists, nums)))
  implicit val arbOptionSetOfDoubles = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Double](Gen.choose(Double.MinValue / 2, Int.MaxValue / 2))) yield OptionSetOfDoubles(opt(exists, nums)))
  implicit val arbOptionSetOfFloats = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield OptionSetOfFloats(opt(exists, nums)))
  implicit val arbOptionSetOfBooleans = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Boolean](Gen.oneOf(true, false))) yield OptionSetOfBooleans(opt(exists, nums)))
  implicit val arbOptionSetOfStrings = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, String](Gen.alphaStr)) yield OptionSetOfStrings(opt(exists, nums)))
  implicit val arbOptionSetOfBytes = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield OptionSetOfBytes(opt(exists, nums)))
  implicit val arbOptionSetOfShorts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield OptionSetOfShorts(opt(exists, nums)))
  implicit val arbOptionSetOfChars = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield OptionSetOfChars(opt(exists, nums)))

}

class OptionSetOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  // Set of Ints
  test("Pickle a case class with an optional list of ints") {
    forAll {
      (obj: OptionSetOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfInts.avsc") === pckl.value)
    }
  }


  test("Unpickle a case class with an optional list of ints") {
    forAll {
      (obj: OptionSetOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfInts.avsc")
        val hydratedObj: OptionSetOfInts = bytes.unpickle[OptionSetOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of ints") {
    forAll {
      (obj: OptionSetOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfInts = pckl.unpickle[OptionSetOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Longs
  test("Pickle a case class with an optional list of Longs") {
    forAll {
      (obj: OptionSetOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Longs") {
    forAll {
      (obj: OptionSetOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfLongs.avsc")
        val hydratedObj: OptionSetOfLongs = bytes.unpickle[OptionSetOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Longs") {
    forAll {
      (obj: OptionSetOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfLongs = pckl.unpickle[OptionSetOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Doubles
  test("Pickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OptionSetOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OptionSetOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfDoubles.avsc")
        val hydratedObj: OptionSetOfDoubles = bytes.unpickle[OptionSetOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Doubles") {
    forAll {
      (obj: OptionSetOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfDoubles = pckl.unpickle[OptionSetOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Floats
  test("Pickle a case class with an optional list of Floats") {
    forAll {
      (obj: OptionSetOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Floats") {
    forAll {
      (obj: OptionSetOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfFloats.avsc")
        val hydratedObj: OptionSetOfFloats = bytes.unpickle[OptionSetOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Floats") {
    forAll {
      (obj: OptionSetOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfFloats = pckl.unpickle[OptionSetOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Booleans
  test("Pickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OptionSetOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OptionSetOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfBooleans.avsc")
        val hydratedObj: OptionSetOfBooleans = bytes.unpickle[OptionSetOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Booleans") {
    forAll {
      (obj: OptionSetOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfBooleans = pckl.unpickle[OptionSetOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Strings
  test("Pickle a case class with an optional list of Strings") {
    forAll {
      (obj: OptionSetOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Strings") {
    forAll {
      (obj: OptionSetOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfStrings.avsc")
        val hydratedObj: OptionSetOfStrings = bytes.unpickle[OptionSetOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Strings") {
    forAll {
      (obj: OptionSetOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfStrings = pckl.unpickle[OptionSetOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Bytes
  test("Pickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OptionSetOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OptionSetOfBytes) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfBytes.avsc")
        val hydratedObj: OptionSetOfBytes = bytes.unpickle[OptionSetOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Bytes") {
    forAll {
      (obj: OptionSetOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfBytes = pckl.unpickle[OptionSetOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Chars
  test("Pickle a case class with an optional list of Chars") {
    forAll {
      (obj: OptionSetOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Chars") {
    forAll {
      (obj: OptionSetOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfChars.avsc")
        val hydratedObj: OptionSetOfChars = bytes.unpickle[OptionSetOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Chars") {
    forAll {
      (obj: OptionSetOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfChars = pckl.unpickle[OptionSetOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Shorts
  test("Pickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OptionSetOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OptionSetOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfShorts.avsc")
        val hydratedObj: OptionSetOfShorts = bytes.unpickle[OptionSetOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Shorts") {
    forAll {
      (obj: OptionSetOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: OptionSetOfShorts = pckl.unpickle[OptionSetOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  private def generateBytesFromAvro(value: Option[Set[_]], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) => record.put("list", x.asJavaCollection)
      case _ => record.put("list", null)
    }
    convertToBytes(schema, record)
  }


}
