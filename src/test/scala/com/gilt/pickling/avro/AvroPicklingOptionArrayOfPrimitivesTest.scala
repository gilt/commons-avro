package com.gilt.pickling.avro

import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.{Assertions, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import org.apache.avro.generic.GenericData
import scala.pickling._
import java.util.{List => JList}
import scala.Some
import com.gilt.pickling.avro.AvroPicklingOptionArrayOfPrimitivesTest.OptionArrayOfInts
import scala.collection.JavaConversions._

object AvroPicklingOptionArrayOfPrimitivesTest {

  case class OptionArrayOfInts(list: Option[Array[Int]])
  case class OptionArrayOfLongs(list: Option[Array[Long]])
  case class OptionArrayOfDoubles(list: Option[Array[Double]])
  case class OptionArrayOfFloats(list: Option[Array[Float]])
  case class OptionArrayOfBooleans(list: Option[Array[Boolean]])
  case class OptionArrayOfStrings(list: Option[Array[String]])
  case class OptionArrayOfBytes(list: Option[Array[Byte]])
  case class OptionArrayOfShorts(list: Option[Array[Short]])
  case class OptionArrayOfChars(list: Option[Array[Char]])

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

class AvroPicklingOptionArrayOfPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

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

  private def generateBytesFromAvro(value: Option[JList[_]], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) => record.put("list", x)
      case _ => record.put("list", null)
    }
    try {
      convertToBytes(schema, record)
    } catch {
      case e: Throwable =>
        println(e)
        throw e
    }
  }
}
