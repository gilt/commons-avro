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

  implicit val arbOptionSetOfInts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield OpSetOfInts(opt(exists, nums)))
  implicit val arbOptionSetOfLongs = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield OpSetOfLongs(opt(exists, nums)))
  implicit val arbOptionSetOfDoubles = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Double](Gen.choose(Double.MinValue / 2, Int.MaxValue / 2))) yield OpSetOfDoubles(opt(exists, nums)))
  implicit val arbOptionSetOfFloats = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield OpSetOfFloats(opt(exists, nums)))
  implicit val arbOptionSetOfBooleans = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Boolean](Gen.oneOf(true, false))) yield OpSetOfBooleans(opt(exists, nums)))
  implicit val arbOptionSetOfStrings = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, String](Gen.alphaStr)) yield OpSetOfStrings(opt(exists, nums)))
  implicit val arbOptionSetOfBytes = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield OpSetOfBytes(opt(exists, nums)))
  implicit val arbOptionSetOfShorts = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield OpSetOfShorts(opt(exists, nums)))
  implicit val arbOptionSetOfChars = Arbitrary(for (exists <- Gen.oneOf(true, false); nums <- Gen.containerOf[Set, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield OpSetOfChars(opt(exists, nums)))

}

class OptionSetOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks{

  // Set of Ints
  test("Pickle a case class with an optional list of ints") {
    forAll {
      (obj: OpSetOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfInts.avsc") === pckl.value)
    }
  }


  test("Unpickle a case class with an optional list of ints") {
    forAll {
      (obj: OpSetOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfInts.avsc")
        val hydratedObj: OpSetOfInts = bytes.unpickle[OpSetOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of ints") {
    forAll {
      (obj: OpSetOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfInts = pckl.unpickle[OpSetOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Longs
  test("Pickle a case class with an optional list of Longs") {
    forAll {
      (obj: OpSetOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Longs") {
    forAll {
      (obj: OpSetOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfLongs.avsc")
        val hydratedObj: OpSetOfLongs = bytes.unpickle[OpSetOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Longs") {
    forAll {
      (obj: OpSetOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfLongs = pckl.unpickle[OpSetOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Doubles
  test("Pickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OpSetOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Doubles") {
    forAll {
      (obj: OpSetOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfDoubles.avsc")
        val hydratedObj: OpSetOfDoubles = bytes.unpickle[OpSetOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Doubles") {
    forAll {
      (obj: OpSetOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfDoubles = pckl.unpickle[OpSetOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Floats
  test("Pickle a case class with an optional list of Floats") {
    forAll {
      (obj: OpSetOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Floats") {
    forAll {
      (obj: OpSetOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfFloats.avsc")
        val hydratedObj: OpSetOfFloats = bytes.unpickle[OpSetOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Floats") {
    forAll {
      (obj: OpSetOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfFloats = pckl.unpickle[OpSetOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Booleans
  test("Pickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OpSetOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Booleans") {
    forAll {
      (obj: OpSetOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfBooleans.avsc")
        val hydratedObj: OpSetOfBooleans = bytes.unpickle[OpSetOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Booleans") {
    forAll {
      (obj: OpSetOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfBooleans = pckl.unpickle[OpSetOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Strings
  test("Pickle a case class with an optional list of Strings") {
    forAll {
      (obj: OpSetOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Strings") {
    forAll {
      (obj: OpSetOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-sets/OptionSetOfStrings.avsc")
        val hydratedObj: OpSetOfStrings = bytes.unpickle[OpSetOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Strings") {
    forAll {
      (obj: OpSetOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfStrings = pckl.unpickle[OpSetOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Bytes
  test("Pickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OpSetOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Bytes") {
    forAll {
      (obj: OpSetOfBytes) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfBytes.avsc")
        val hydratedObj: OpSetOfBytes = bytes.unpickle[OpSetOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Bytes") {
    forAll {
      (obj: OpSetOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfBytes = pckl.unpickle[OpSetOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Chars
  test("Pickle a case class with an optional list of Chars") {
    forAll {
      (obj: OpSetOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Chars") {
    forAll {
      (obj: OpSetOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfChars.avsc")
        val hydratedObj: OpSetOfChars = bytes.unpickle[OpSetOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Chars") {
    forAll {
      (obj: OpSetOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfChars = pckl.unpickle[OpSetOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Set of Shorts
  test("Pickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OpSetOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional list of Shorts") {
    forAll {
      (obj: OpSetOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.map(_.toInt)), "/avro/option-sets/OptionSetOfShorts.avsc")
        val hydratedObj: OpSetOfShorts = bytes.unpickle[OpSetOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional list of Shorts") {
    forAll {
      (obj: OpSetOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: OpSetOfShorts = pckl.unpickle[OpSetOfShorts]
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
