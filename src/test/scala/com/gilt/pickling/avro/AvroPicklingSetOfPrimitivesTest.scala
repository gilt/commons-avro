package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import avro._
import scala.collection.JavaConversions._
import java.util.{Set => JSet}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

object AvroPicklingSetOfPrimitivesTest {

  case class SetOfInts(list: Set[Int])

  case class SetOfLongs(list: Set[Long])

  case class SetOfDoubles(list: Set[Double])

  case class SetOfFloats(list: Set[Float])

  case class SetOfBooleans(list: Set[Boolean])

  case class SetOfStrings(list: Set[String])

  case class SetOfBytes(list: Set[Byte])

  case class SetOfShorts(list: Set[Short])

  case class SetOfChars(list: Set[Char])

  import org.scalacheck.{Gen, Arbitrary}

  implicit val arbSetOfInts = Arbitrary(for (set <- Gen.containerOf[Set, Int](Gen.choose(Int.MinValue, Int.MaxValue))) yield SetOfInts(set))
  implicit val arbSetOfLongs = Arbitrary(for (set <- Gen.containerOf[Set, Long](Gen.choose(Long.MinValue, Long.MaxValue))) yield SetOfLongs(set))
  implicit val arbSetOfDoubles = Arbitrary(for (set <- Gen.containerOf[Set, Double](Gen.chooseNum(Double.MinValue/2, Double.MaxValue/2))) yield SetOfDoubles(set))
  implicit val arbSetOfFloats = Arbitrary(for (set <- Gen.containerOf[Set, Float](Gen.chooseNum(Float.MinValue, Float.MaxValue))) yield SetOfFloats(set))
  implicit val arbSetOfBooleans = Arbitrary(for (set <- Gen.containerOf[Set, Boolean](Gen.oneOf(true, false))) yield SetOfBooleans(set))
  implicit val arbSetOfStrings = Arbitrary(for (set <- Gen.containerOf[Set, String](Gen.alphaStr)) yield SetOfStrings(set))
  implicit val arbSetOfBytes = Arbitrary(for (set <- Gen.containerOf[Set, Byte](Gen.choose(Byte.MinValue, Byte.MaxValue))) yield SetOfBytes(set))
  implicit val arbSetOfShorts = Arbitrary(for (set <- Gen.containerOf[Set, Short](Gen.choose(Short.MinValue, Short.MaxValue))) yield SetOfShorts(set))
  implicit val arbSetOfChars = Arbitrary(for (set <- Gen.containerOf[Set, Char](Gen.choose(Char.MinValue, Char.MaxValue))) yield SetOfChars(set))
}

class AvroPicklingSetOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  import AvroPicklingSetOfPrimitivesTest._

  // Array of Ints
  test("Pickle a case class with an set of ints") {
    forAll {
      (obj: SetOfInts) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/sets/SetOfInts.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an set of ints") {
    forAll {
      (obj: SetOfInts) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/sets/SetOfInts.avsc")
        val hydratedObj: SetOfInts = bytes.unpickle[SetOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an set of ints") {
    forAll {
      (obj: SetOfInts) =>
        val pckl = obj.pickle
        val hydratedObj: SetOfInts = pckl.unpickle[SetOfInts]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Longs
  test("Pickle a case class with an set of longs") {
    forAll {
      (obj: SetOfLongs) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/sets/SetOfLongs.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an set of longs") {
    forAll {
      (obj: SetOfLongs) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/sets/SetOfLongs.avsc")
        val hydratedObj: SetOfLongs = bytes.unpickle[SetOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an set of longs") {
    forAll {
      (obj: SetOfLongs) =>
        val pckl = obj.pickle
        val hydratedObj: SetOfLongs = pckl.unpickle[SetOfLongs]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Doubles
  test("Pickle a case class with an set of doubles") {
    forAll {
      (obj: SetOfDoubles) =>
        val pckl = obj.pickle
        assert(generateBytesFromAvro(obj.list, "/avro/sets/SetOfDoubles.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with an set of doubles") {
    forAll {
      (obj: SetOfDoubles) =>
        val bytes = generateBytesFromAvro(obj.list, "/avro/sets/SetOfDoubles.avsc")
        val hydratedObj: SetOfDoubles = bytes.unpickle[SetOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  test("Round trip a case class with an set of doubles") {
    forAll {
      (obj: SetOfDoubles) =>
        val pckl = obj.pickle
        val hydratedObj: SetOfDoubles = pckl.unpickle[SetOfDoubles]
        assert(obj.list === hydratedObj.list)
    }
  }

  // Array of Floats
  test("Pickle a case class with an set of floats") {
    val obj = SetOfFloats(Set(1.1F, 2.2F, 3.3F, 4.4F))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/sets/SetOfFloats.avsc") === pckl.value)
  }

  test("Unpickle a case class with an set of floats") {
    val obj = SetOfFloats(Set(1.1F, 2.2F, 3.3F, 4.4F))
    val bytes = generateBytesFromAvro(obj.list, "/avro/sets/SetOfFloats.avsc")
    val hydratedObj: SetOfFloats = bytes.unpickle[SetOfFloats]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an set of floats") {
    val obj = SetOfFloats(Set(1.1F, 2.2F, 3.3F, 4.4F))
    val pckl = obj.pickle
    val hydratedObj: SetOfFloats = pckl.unpickle[SetOfFloats]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Floats
  test("Pickle a case class with an set of boolean") {
    val obj = SetOfBooleans(Set(true, false, true, true))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/sets/SetOfBooleans.avsc") === pckl.value)
  }

  test("Unpickle a case class with an set of boolean") {
    val obj = SetOfBooleans(Set(true, false, true, true))
    val bytes = generateBytesFromAvro(obj.list, "/avro/sets/SetOfBooleans.avsc")
    val hydratedObj: SetOfBooleans = bytes.unpickle[SetOfBooleans]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an set of boolean") {
    val obj = SetOfBooleans(Set(true, false, true, true))
    val pckl = obj.pickle
    val hydratedObj: SetOfBooleans = pckl.unpickle[SetOfBooleans]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Strings
  test("Pickle a case class with an set of string") {
    val obj = SetOfStrings(Set[String]("a", "b", "c", "d"))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list, "/avro/sets/SetOfStrings.avsc") === pckl.value)
  }

  test("Unpickle a case class with an set of string") {
    val obj = SetOfStrings(Set[String]("a", "b", "c", "d"))
    val bytes = generateBytesFromAvro(obj.list, "/avro/sets/SetOfStrings.avsc")
    val hydratedObj: SetOfStrings = bytes.unpickle[SetOfStrings]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an set of string") {
    val obj = SetOfStrings(Set[String]("a", "b", "c", "d"))
    val pckl = obj.pickle
    val hydratedObj: SetOfStrings = pckl.unpickle[SetOfStrings]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Bytes
  // TODO A better solution is to write this a as bytebuffer
  test("Pickle a case class with an set of bytes") {
    val obj = SetOfBytes(Set(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/sets/SetOfBytes.avsc") === pckl.value)
  }

  test("Unpickle a case class with an set of bytes") {
    val obj = SetOfBytes(Set(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/sets/SetOfBytes.avsc")
    val hydratedObj: SetOfBytes = bytes.unpickle[SetOfBytes]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an set of bytes") {
    val obj = SetOfBytes(Set(1.toByte, 2.toByte, 3.toByte, 4.toByte))
    val pckl = obj.pickle
    val hydratedObj: SetOfBytes = pckl.unpickle[SetOfBytes]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Shorts
  test("Pickle a case class with an set of shorts") {
    val obj = SetOfShorts(Set(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/sets/SetOfShorts.avsc") === pckl.value)
  }

  test("Unpickle a case class with an set of shorts") {
    val obj = SetOfShorts(Set(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/sets/SetOfShorts.avsc")
    val hydratedObj: SetOfShorts = bytes.unpickle[SetOfShorts]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an set of shorts") {
    val obj = SetOfShorts(Set(1.toShort, 2.toShort, 3.toShort, 4.toShort))
    val pckl = obj.pickle
    val hydratedObj: SetOfShorts = pckl.unpickle[SetOfShorts]
    assert(obj.list === hydratedObj.list)
  }

  // Array of Chars
  test("Pickle a case class with an set of chars") {
    val obj = SetOfChars(Set(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val pckl = obj.pickle
    assert(generateBytesFromAvro(obj.list.map(_.toInt), "/avro/sets/SetOfChars.avsc") === pckl.value)
  }

  test("Unpickle a case class with an set of chars") {
    val obj = SetOfChars(Set(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val bytes = generateBytesFromAvro(obj.list.map(_.toInt), "/avro/sets/SetOfChars.avsc")
    val hydratedObj: SetOfChars = bytes.unpickle[SetOfChars]
    assert(obj.list === hydratedObj.list)
  }

  test("Round trip a case class with an set of chars") {
    val obj = SetOfChars(Set(1.toChar, 2.toChar, 3.toChar, 4.toChar))
    val pckl = obj.pickle
    val hydratedObj: SetOfChars = pckl.unpickle[SetOfChars]
    assert(obj.list === hydratedObj.list)
  }

  private def generateBytesFromAvro(value: JSet[_], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    record.put("list", value) // need java collection at this point
    convertToBytes(schema, record)
  }

}
