package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils
import TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import scala.collection.JavaConversions._
import java.util.{Map => JMap}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.gilt.pickling.TestObjs._
import org.scalacheck.{Arbitrary, Gen}

object MapOfPrimitivesTest {
  implicit val arbMapOfInts = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield MapOfInts(keys.zip(values).toMap))
  implicit val arbMapOfLongs = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield MapOfLongs(keys.zip(values).toMap))
  implicit val arbMapOfDoubles = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Double](Gen.choose(Double.MinValue / 2, Double.MaxValue / 2))) yield MapOfDoubles(keys.zip(values).toMap))
  implicit val arbMapOfFloats = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Float](Gen.choose(Float.MinValue, Float.MaxValue))) yield MapOfFloats(keys.zip(values).toMap))
  implicit val arbMapOfBooleans = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Boolean](Gen.oneOf(true, false))) yield MapOfBooleans(keys.zip(values).toMap))
  implicit val arbMapOfStrings = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, String](Gen.alphaStr)) yield MapOfStrings(keys.zip(values).toMap))
  implicit val arbMapOfBytes = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield MapOfBytes(keys.zip(values).toMap))
  implicit val arbMapOfShorts = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield MapOfShorts(keys.zip(values).toMap))
  implicit val arbMapOfChars = Arbitrary(for (keys <- Gen.containerOf[List, String](Gen.alphaStr); values <- Gen.containerOf[List, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield MapOfChars(keys.zip(values).toMap))
}

class MapOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  import MapOfPrimitivesTest._

  //Negative
  test("Pickle a case class with a map other than Map[String, _] fails") {
    intercept[PicklingException] {
      MapOfIntInts(Map(1 -> 2)).pickle
    }
  }

  // Array of Ints
  test("Pickle a case class with an map of ints") {
    forAll {
      (obj: MapOfInts) =>
        val pckl = obj.pickle
        val depickled = generateBytesFromAvro(obj.list, "/avro/maps/MapOfInts.avsc")
        assert(depickled === pckl.value)
    }
  }

  test("Unpickle a case class with an map of ints") {
    forAll {
      (obj: MapOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/maps/MapOfInts.avsc")
        val hydratedObj: MapOfInts = bytes.unpickle[MapOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of ints") {
    forAll {
      (obj: MapOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfInts = pckl.unpickle[MapOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Longs
  test("Pickle a case class with an map of longs") {
    forAll {
      (obj: MapOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/maps/MapOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of longs") {
    forAll {
      (obj: MapOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/maps/MapOfLongs.avsc")
        val hydratedObj: MapOfLongs = bytes.unpickle[MapOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of longs") {
    forAll {
      (obj: MapOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfLongs = pckl.unpickle[MapOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Doubles
  test("Pickle a case class with an map of doubles") {
    forAll {
      (obj: MapOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/maps/MapOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of doubles") {
    forAll {
      (obj: MapOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/maps/MapOfDoubles.avsc")
        val hydratedObj: MapOfDoubles = bytes.unpickle[MapOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of doubles") {
    forAll {
      (obj: MapOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfDoubles = pckl.unpickle[MapOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an map of floats") {
    forAll {
      (obj: MapOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/maps/MapOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of floats") {
    forAll {
      (obj: MapOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/maps/MapOfFloats.avsc")
        val hydratedObj: MapOfFloats = bytes.unpickle[MapOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of floats") {
    forAll {
      (obj: MapOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfFloats = pckl.unpickle[MapOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an map of boolean") {
    forAll {
      (obj: MapOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/maps/MapOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of boolean") {
    forAll {
      (obj: MapOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/maps/MapOfBooleans.avsc")
        val hydratedObj: MapOfBooleans = bytes.unpickle[MapOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of boolean") {
    forAll {
      (obj: MapOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfBooleans = pckl.unpickle[MapOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Strings
  test("Pickle a case class with an map of string") {
    forAll {
      (obj: MapOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/maps/MapOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of string") {
    forAll {
      (obj: MapOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/maps/MapOfStrings.avsc")
        val hydratedObj: MapOfStrings = bytes.unpickle[MapOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of string") {
    forAll {
      (obj: MapOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfStrings = pckl.unpickle[MapOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Bytes
  // TODO A better solution is to write this a as bytebuffer
  test("Pickle a case class with an map of bytes") {
    forAll {
      (obj: MapOfBytes) =>
        val pckl = obj.pickle

        assert(generateBytesFromAvro(obj.list.mapValues(_.toInt), "/avro/maps/MapOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of bytes") {
    forAll {
      (obj: MapOfBytes) =>
        val bytes = generateBytesFromAvro(obj.list.mapValues(_.toInt), "/avro/maps/MapOfBytes.avsc")
        val hydratedObj: MapOfBytes = bytes.unpickle[MapOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of bytes") {
    forAll {
      (obj: MapOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfBytes = pckl.unpickle[MapOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Shorts
  test("Pickle a case class with an map of shorts") {
    forAll {
      (obj: MapOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.mapValues(_.toInt), "/avro/maps/MapOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of shorts") {
    forAll {
      (obj: MapOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.mapValues(_.toInt), "/avro/maps/MapOfShorts.avsc")
        val hydratedObj: MapOfShorts = bytes.unpickle[MapOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of shorts") {
    forAll {
      (obj: MapOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfShorts = pckl.unpickle[MapOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Chars
  test("Pickle a case class with an map of chars") {
    forAll {
      (obj: MapOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.mapValues(_.toInt), "/avro/maps/MapOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an map of chars") {
    forAll {
      (obj: MapOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.mapValues(_.toInt), "/avro/maps/MapOfChars.avsc")
        val hydratedObj: MapOfChars = bytes.unpickle[MapOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an map of chars") {
    forAll {
      (obj: MapOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: MapOfChars = pckl.unpickle[MapOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  private def generateBytesFromAvro(value: JMap[String, Any], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }
}
