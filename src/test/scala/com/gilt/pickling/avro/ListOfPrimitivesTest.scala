package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils
import TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import scala.collection.JavaConversions._
import java.util.{List => JList}
import ListOfPrimitivesTest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.gilt.pickling.TestObjs._
import org.scalacheck.{Gen, Arbitrary}

object ListOfPrimitivesTest {
  implicit val arbListOfInts = Arbitrary(for (list <- Gen.containerOf[List, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield ListOfInts(list))
  implicit val arbListOfLongs = Arbitrary(for (list <- Gen.containerOf[List, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield ListOfLongs(list))
  implicit val arbListOfDoubles = Arbitrary(for (list <- Gen.containerOf[List, Double](Gen.chooseNum(Double.MinValue / 2, Double.MaxValue / 2))) yield ListOfDoubles(list))
  implicit val arbListOfFloats = Arbitrary(for (list <- Gen.containerOf[List, Float](Gen.chooseNum(Float.MinValue, Float.MaxValue))) yield ListOfFloats(list))
  implicit val arbListOfBooleans = Arbitrary(for (list <- Gen.containerOf[List, Boolean](Gen.oneOf(true, false))) yield ListOfBooleans(list))
  implicit val arbListOfStrings = Arbitrary(for (list <- Gen.containerOf[List, String](Gen.alphaStr)) yield ListOfStrings(list))
  implicit val arbListOfBytes = Arbitrary(for (list <- Gen.containerOf[List, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield ListOfBytes(list))
  implicit val arbListOfShorts = Arbitrary(for (list <- Gen.containerOf[List, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield ListOfShorts(list))
  implicit val arbListOfChars = Arbitrary(for (list <- Gen.containerOf[List, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield ListOfChars(list))
}

class ListOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  // Array of Ints
  test("Pickle a case class with an list of ints") {
    forAll {
      (obj: ListOfInts) =>
        val pckl = obj.pickle
        val depickled = generateBytesFromAvro(obj.list, "/avro/lists/ListOfInts.avsc")
        assert(depickled === pckl.value)
    }
  }

  test("Unpickle a case class with an list of ints") {
    forAll {
      (obj: ListOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfInts.avsc")
        val hydratedObj: ListOfInts = bytes.unpickle[ListOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of ints") {
    forAll {
      (obj: ListOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfInts = pckl.unpickle[ListOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Longs
  test("Pickle a case class with an list of longs") {
    forAll {
      (obj: ListOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of longs") {
    forAll {
      (obj: ListOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfLongs.avsc")
        val hydratedObj: ListOfLongs = bytes.unpickle[ListOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of longs") {
    forAll {
      (obj: ListOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfLongs = pckl.unpickle[ListOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Doubles
  test("Pickle a case class with an list of doubles") {
    forAll {
      (obj: ListOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of doubles") {
    forAll {
      (obj: ListOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfDoubles.avsc")
        val hydratedObj: ListOfDoubles = bytes.unpickle[ListOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of doubles") {
    forAll {
      (obj: ListOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfDoubles = pckl.unpickle[ListOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an list of floats") {
    forAll {
      (obj: ListOfFloats) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfFloats.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of floats") {
    forAll {
      (obj: ListOfFloats) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfFloats.avsc")
        val hydratedObj: ListOfFloats = bytes.unpickle[ListOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of floats") {
    forAll {
      (obj: ListOfFloats) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfFloats = pckl.unpickle[ListOfFloats]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an list of boolean") {
    forAll {
      (obj: ListOfBooleans) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfBooleans.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of boolean") {
    forAll {
      (obj: ListOfBooleans) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfBooleans.avsc")
        val hydratedObj: ListOfBooleans = bytes.unpickle[ListOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of boolean") {
    forAll {
      (obj: ListOfBooleans) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfBooleans = pckl.unpickle[ListOfBooleans]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Strings
  test("Pickle a case class with an list of string") {
    forAll {
      (obj: ListOfStrings) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/lists/ListOfStrings.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of string") {
    forAll {
      (obj: ListOfStrings) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/lists/ListOfStrings.avsc")
        val hydratedObj: ListOfStrings = bytes.unpickle[ListOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of string") {
    forAll {
      (obj: ListOfStrings) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfStrings = pckl.unpickle[ListOfStrings]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Bytes
  // TODO A better solution is to write this a as bytebuffer
  test("Pickle a case class with an list of bytes") {
    forAll {
      (obj: ListOfBytes) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfBytes.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of bytes") {
    forAll {
      (obj: ListOfBytes) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfBytes.avsc")
        val hydratedObj: ListOfBytes = bytes.unpickle[ListOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of bytes") {
    forAll {
      (obj: ListOfBytes) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfBytes = pckl.unpickle[ListOfBytes]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Shorts
  test("Pickle a case class with an list of shorts") {
    forAll {
      (obj: ListOfShorts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfShorts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of shorts") {
    forAll {
      (obj: ListOfShorts) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfShorts.avsc")
        val hydratedObj: ListOfShorts = bytes.unpickle[ListOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of shorts") {
    forAll {
      (obj: ListOfShorts) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfShorts = pckl.unpickle[ListOfShorts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Chars
  test("Pickle a case class with an list of chars") {
    forAll {
      (obj: ListOfChars) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfChars.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an list of chars") {
    forAll {
      (obj: ListOfChars) =>
        val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/lists/ListOfChars.avsc")
        val hydratedObj: ListOfChars = bytes.unpickle[ListOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an list of chars") {
    forAll {
      (obj: ListOfChars) =>
        val pckl = obj.pickle
        val hydratedObj: ListOfChars = pckl.unpickle[ListOfChars]
        assert(obj.list === hydratedObj.list)
    }
  }

  private def generateBytesFromAvro(value: JList[Any], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }
}
