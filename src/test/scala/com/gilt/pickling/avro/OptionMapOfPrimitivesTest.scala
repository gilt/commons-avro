package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils
import TestUtils._
import org.apache.avro.generic.GenericData
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.gilt.pickling.TestObjs._
import org.scalacheck.{Gen, Arbitrary}
import scala.pickling._
import java.util.{Map => JMap}

object OptionMapOfPrimitivesTest {
  def opt[T](exists: Boolean, value: T) = if (exists) Some(value) else None

  implicit val arbOptionMapOfInts = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield OptionMapOfInts(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfLongs = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield OptionMapOfLongs(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfDoubles = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Double](Gen.choose(Double.MinValue / 2, Double.MaxValue / 2))) yield OptionMapOfDoubles(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfFloats = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield OptionMapOfFloats(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfBooleans = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Boolean](Gen.oneOf(true, false))) yield OptionMapOfBooleans(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfStrings = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, String](Gen.alphaStr)) yield OptionMapOfStrings(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfBytes = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield OptionMapOfBytes(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfShorts = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield OptionMapOfShorts(opt(exists, keys.zip(values).toMap)))
  implicit val arbOptionMapOfChars = Arbitrary(for (exists <- Gen.oneOf(true, false); keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield OptionMapOfChars(opt(exists, keys.zip(values).toMap)))
}

class OptionMapOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {
  import OptionMapOfPrimitivesTest._

  //Negative
  test("Pickle a case class with a map other than Map[String, _] fails") {
    intercept[PicklingException] {
      OptionMapOfIntInts(Some(Map(1 -> 2))).pickle
    }
  }

  // Array of Ints
  test("Pickle a case class with an optional map of ints") {
    forAll {
      (obj: OptionMapOfInts) =>
        val pckl = obj.pickle
        val depickled = generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfInts.avsc")
        assert(depickled === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of ints") {
    forAll {
      (obj: OptionMapOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfInts.avsc")
        val  hydratedObj = bytes.unpickle[OptionMapOfInts]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of ints") {
    forAll {
      (obj: OptionMapOfInts) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfInts = pckl.unpickle[OptionMapOfInts]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Longs
  test("Pickle a case class with an optional map of longs") {
    forAll {
      (obj: OptionMapOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of longs") {
    forAll {
      (obj: OptionMapOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfLongs.avsc")
        val  hydratedObj: OptionMapOfLongs = bytes.unpickle[OptionMapOfLongs]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of longs") {
    forAll {
      (obj: OptionMapOfLongs) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfLongs = pckl.unpickle[OptionMapOfLongs]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Doubles
  test("Pickle a case class with an optional map of doubles") {
    forAll {
      (obj: OptionMapOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of doubles") {
    forAll {
      (obj: OptionMapOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfDoubles.avsc")
        val  hydratedObj: OptionMapOfDoubles = bytes.unpickle[OptionMapOfDoubles]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of doubles") {
    forAll {
      (obj: OptionMapOfDoubles) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfDoubles = pckl.unpickle[OptionMapOfDoubles]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an optional map of floats") {
    forAll {
      (obj: OptionMapOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of floats") {
    forAll {
      (obj: OptionMapOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfFloats.avsc")
        val  hydratedObj: OptionMapOfFloats = bytes.unpickle[OptionMapOfFloats]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of floats") {
    forAll {
      (obj: OptionMapOfFloats) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfFloats = pckl.unpickle[OptionMapOfFloats]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an optional map of boolean") {
    forAll {
      (obj: OptionMapOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of boolean") {
    forAll {
      (obj: OptionMapOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfBooleans.avsc")
        val  hydratedObj: OptionMapOfBooleans = bytes.unpickle[OptionMapOfBooleans]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of boolean") {
    forAll {
      (obj: OptionMapOfBooleans) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfBooleans = pckl.unpickle[OptionMapOfBooleans]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Strings
  test("Pickle a case class with an optional map of string") {
    forAll {
      (obj: OptionMapOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of string") {
    forAll {
      (obj: OptionMapOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/option-maps/OptionMapOfStrings.avsc")
        val  hydratedObj: OptionMapOfStrings = bytes.unpickle[OptionMapOfStrings]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of string") {
    forAll {
      (obj: OptionMapOfStrings) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfStrings = pckl.unpickle[OptionMapOfStrings]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Bytes
  // TODO A better solution is to write this a as bytebuffer
  test("Pickle a case class with an optional map of bytes") {
    forAll {
      (obj: OptionMapOfBytes) =>
        val pckl = obj.pickle

        assert(generateBytesFromAvro(obj.list.map(_.mapValues(_.toInt)), "/avro/option-maps/OptionMapOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of bytes") {
    forAll {
      (obj: OptionMapOfBytes) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.mapValues(_.toInt)), "/avro/option-maps/OptionMapOfBytes.avsc")
        val  hydratedObj: OptionMapOfBytes = bytes.unpickle[OptionMapOfBytes]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of bytes") {
    forAll {
      (obj: OptionMapOfBytes) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfBytes = pckl.unpickle[OptionMapOfBytes]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Shorts
  test("Pickle a case class with an optional map of shorts") {
    forAll {
      (obj: OptionMapOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.mapValues(_.toInt)), "/avro/option-maps/OptionMapOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of shorts") {
    forAll {
      (obj: OptionMapOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.mapValues(_.toInt)), "/avro/option-maps/OptionMapOfShorts.avsc")
        val  hydratedObj: OptionMapOfShorts = bytes.unpickle[OptionMapOfShorts]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of shorts") {
    forAll {
      (obj: OptionMapOfShorts) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfShorts = pckl.unpickle[OptionMapOfShorts]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  // Array of Chars
  test("Pickle a case class with an optional map of chars") {
    forAll {
      (obj: OptionMapOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.mapValues(_.toInt)), "/avro/option-maps/OptionMapOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an optional map of chars") {
    forAll {
      (obj: OptionMapOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.mapValues(_.toInt)), "/avro/option-maps/OptionMapOfChars.avsc")
        val  hydratedObj: OptionMapOfChars = bytes.unpickle[OptionMapOfChars]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  test("Round trip a case class with an optional map of chars") {
    forAll {
      (obj: OptionMapOfChars) =>
        val pckl = obj.pickle
        val  hydratedObj: OptionMapOfChars = pckl.unpickle[OptionMapOfChars]
        assert(obj.list ===  hydratedObj.list)
    }
  }

  private def generateBytesFromAvro(value: Option[Map[String, _]], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) =>
        import scala.collection.JavaConversions.mapAsJavaMap
        val map: JMap[String, _] = x
        record.put("list", map) // need java collection at this point
      case _ => record.put("list", null)
    }

    convertToBytes(schema, record)
  }
}
