package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.avro.TestUtils._
import com.gilt.pickling.avro.AvroPickleSingleOptionalFieldPrimitivesTest.SingleOptionInt
import scala.Some
import org.apache.avro.generic.GenericData
import scala.pickling._
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

object AvroPickleSingleOptionalFieldPrimitivesTest {
  case class SingleOptionInt(id: Option[Int])
  case class SingleOptionLong(id: Option[Long])
  case class SingleOptionDouble(id: Option[Double])
  case class SingleOptionFloat(id: Option[Float])
  case class SingleOptionBoolean(id: Option[Boolean])
  case class SingleOptionString(id: Option[String])
  case class SingleOptionByte(id: Option[Byte])
  case class SingleOptionShort(id: Option[Short])
  case class SingleOptionChar(id: Option[Char])

  implicit val arbSingleOptionInt = Arbitrary(for (num <- Gen.choose(Int.MinValue, Int.MaxValue)) yield SingleOptionInt(Some(num)))
  implicit val arbSingleOptionLong = Arbitrary(for (num <- Gen.choose(Long.MinValue, Long.MaxValue)) yield SingleOptionLong(Some(num)))
  implicit val arbSingleOptionDouble = Arbitrary(for (num <- Gen.choose(Double.MinValue/2, Double.MaxValue/2)) yield SingleOptionDouble(Some(num)))
  implicit val arbSingleOptionFloat = Arbitrary(for (num <- Gen.choose(Float.MinValue, Float.MaxValue)) yield SingleOptionFloat(Some(num)))
  implicit val arbSingleOptionBoolean = Arbitrary(for (num <- Gen.oneOf(true, false)) yield SingleOptionBoolean(Some(num)))
  implicit val arbSingleOptionString = Arbitrary(for (num <- Gen.alphaStr) yield SingleOptionString(Some(num)))
  implicit val arbSingleOptionByte = Arbitrary(for (num <- Gen.choose(Byte.MinValue, Byte.MaxValue)) yield SingleOptionByte(Some(num)))
  implicit val arbSingleOptionShort = Arbitrary(for (num <- Gen.choose(Short.MinValue, Short.MaxValue)) yield SingleOptionShort(Some(num)))
  implicit val arbSingleOptionChar = Arbitrary(for (num <- Gen.choose(Char.MinValue, Char.MaxValue)) yield SingleOptionChar(Some(num)))

}

class AvroPickleSingleOptionalFieldPrimitivesTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks{

  //Single Int
  test("Pickle a case class with a single optional int field") {
    forAll {
      (obj: SingleOptionInt) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionInt.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional int field") {
    forAll {
      (int: Int) =>
        val bytes = generateSingleValueBytesFromAvro(Some(int), "/avro/option/SingleOptionInt.avsc")
        val obj: SingleOptionInt = bytes.unpickle[SingleOptionInt]
        assert(obj === new SingleOptionInt(Some(int)))
    }
  }

  test("Round trip a case class with a single optional int field") {
    forAll {
      (obj: SingleOptionInt) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionInt = pckl.unpickle[SingleOptionInt]
        assert(hydratedObj === obj)
    }
  }

  test("Pickle a case class with a single none int value") {
    val obj = new SingleOptionInt(None)
    val pckl = obj.pickle
    assert(generateSingleValueBytesFromAvro(None, "/avro/option/SingleOptionInt.avsc") === pckl.value)
  }

  test("Unpickle a case class with a single none int value") {
    val bytes = generateSingleValueBytesFromAvro(None, "/avro/option/SingleOptionInt.avsc")
    val obj: SingleOptionInt = bytes.unpickle[SingleOptionInt]
    assert(obj === new SingleOptionInt(None))
  }

  test("Round trip a case class with a single none int value") {
    val obj = new SingleOptionInt(None)
    val pckl = obj.pickle
    val hydratedObj: SingleOptionInt = pckl.unpickle[SingleOptionInt]
    assert(hydratedObj === obj)
  }

  private def generateSingleValueBytesFromAvro(value: Option[_], schemaFileLocation: String): Array[Byte] = {
    val schema: Schema = retrieveAvroSchemaFromFile(schemaFileLocation)
    val record = new GenericData.Record(schema)
    value match {
      case Some(x) => record.put("id", x)
      case _ => record.put("id", null)
    }
    convertToBytes(schema, record)
  }
}
