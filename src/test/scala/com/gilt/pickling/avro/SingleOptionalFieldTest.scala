package com.gilt.pickling.avro

import org.scalatest.{Assertions, FunSuite}
import org.apache.avro.Schema
import com.gilt.pickling.TestUtils
import TestUtils._
import com.gilt.pickling.avro.SingleOptionalFieldTest._
import scala.Some
import org.apache.avro.generic.GenericData
import scala.pickling._
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import com.gilt.pickling.TestObjs._

object SingleOptionalFieldTest {
  def opt[T](exists: Boolean, value: T) = if (exists) Some(value) else None

  implicit val arbSingleOptionInt = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Int.MinValue, Int.MaxValue)) yield SingleOptionInt(opt(exists,num)))
  implicit val arbSingleOptionLong = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Long.MinValue, Long.MaxValue)) yield SingleOptionLong(opt(exists,num)))
  implicit val arbSingleOptionDouble = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Double.MinValue/2, Double.MaxValue/2)) yield SingleOptionDouble(opt(exists, num)))
  implicit val arbSingleOptionFloat = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Float.MinValue, Float.MaxValue)) yield SingleOptionFloat(opt(exists, num)))
  implicit val arbSingleOptionBoolean = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.oneOf(true, false)) yield SingleOptionBoolean(opt(exists, num)))
  implicit val arbSingleOptionString = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.alphaStr) yield SingleOptionString(opt(exists, num)))
  implicit val arbSingleOptionByte = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Byte.MinValue, Byte.MaxValue)) yield SingleOptionByte(opt(exists, num)))
  implicit val arbSingleOptionShort = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Short.MinValue, Short.MaxValue)) yield SingleOptionShort(opt(exists, num)))
  implicit val arbSingleOptionChar = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Char.MinValue, Char.MaxValue)) yield SingleOptionChar(opt(exists, num)))

}

class SingleOptionalFieldTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  //Single Option Int
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

  //Single Option Long
  test("Pickle a case class with a single optional long field") {
    forAll {
      (obj: SingleOptionLong) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionLong.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional long field") {
    forAll {
      (long: Long) =>
        val bytes = generateSingleValueBytesFromAvro(Some(long), "/avro/option/SingleOptionLong.avsc")
        val obj: SingleOptionLong = bytes.unpickle[SingleOptionLong]
        assert(obj === new SingleOptionLong(Some(long)))
    }
  }

  test("Round trip a case class with a single optional long field") {
    forAll {
      (obj: SingleOptionLong) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionLong = pckl.unpickle[SingleOptionLong]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Double
  test("Pickle a case class with a single optional double field") {
    forAll {
      (obj: SingleOptionDouble) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionDouble.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional double field") {
    forAll {
      (double: Double) =>
        val bytes = generateSingleValueBytesFromAvro(Some(double), "/avro/option/SingleOptionDouble.avsc")
        val obj: SingleOptionDouble = bytes.unpickle[SingleOptionDouble]
        assert(obj === new SingleOptionDouble(Some(double)))
    }
  }

  test("Round trip a case class with a single optional double field") {
    forAll {
      (obj: SingleOptionDouble) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionDouble = pckl.unpickle[SingleOptionDouble]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Float
  test("Pickle a case class with a single optional Float field") {
    forAll {
      (obj: SingleOptionFloat) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionFloat.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Float field") {
    forAll {
      (float: Float) =>
        val bytes = generateSingleValueBytesFromAvro(Some(float), "/avro/option/SingleOptionFloat.avsc")
        val obj: SingleOptionFloat = bytes.unpickle[SingleOptionFloat]
        assert(obj === new SingleOptionFloat(Some(float)))
    }
  }

  test("Round trip a case class with a single optional Float field") {
    forAll {
      (obj: SingleOptionFloat) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionFloat = pckl.unpickle[SingleOptionFloat]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Boolean
  test("Pickle a case class with a single optional Boolean field") {
    forAll {
      (obj: SingleOptionBoolean) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionBoolean.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Boolean field") {
    forAll {
      (boolean: Boolean) =>
        val bytes = generateSingleValueBytesFromAvro(Some(boolean), "/avro/option/SingleOptionBoolean.avsc")
        val obj: SingleOptionBoolean = bytes.unpickle[SingleOptionBoolean]
        assert(obj === new SingleOptionBoolean(Some(boolean)))
    }
  }

  test("Round trip a case class with a single optional Boolean field") {
    forAll {
      (obj: SingleOptionBoolean) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionBoolean = pckl.unpickle[SingleOptionBoolean]
        assert(hydratedObj === obj)
    }
  }

  //Single Option String
  test("Pickle a case class with a single optional String field") {
    forAll {
      (obj: SingleOptionString) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionString.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional String field") {
    forAll {
      (string: String) =>
        val bytes = generateSingleValueBytesFromAvro(Some(string), "/avro/option/SingleOptionString.avsc")
        val obj: SingleOptionString = bytes.unpickle[SingleOptionString]
        assert(obj === new SingleOptionString(Some(string)))
    }
  }

  test("Round trip a case class with a single optional String field") {
    forAll {
      (obj: SingleOptionString) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionString = pckl.unpickle[SingleOptionString]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Byte
  test("Pickle a case class with a single optional Byte field") {
    forAll {
      (obj: SingleOptionByte) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id.map(_.toInt), "/avro/option/SingleOptionByte.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Byte field") {
    forAll {
      (byte: Byte) =>
        val bytes = generateSingleValueBytesFromAvro(Some(byte.toInt), "/avro/option/SingleOptionByte.avsc")
        val obj: SingleOptionByte = bytes.unpickle[SingleOptionByte]
        assert(obj === new SingleOptionByte(Some(byte)))
    }
  }

  test("Round trip a case class with a single optional Byte field") {
    forAll {
      (obj: SingleOptionByte) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionByte = pckl.unpickle[SingleOptionByte]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Short
  test("Pickle a case class with a single optional Short field") {
    forAll {
      (obj: SingleOptionShort) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id.map(_.toInt), "/avro/option/SingleOptionShort.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Short field") {
    forAll {
      (short: Short) =>
        val bytes = generateSingleValueBytesFromAvro(Some(short.toInt), "/avro/option/SingleOptionShort.avsc")
        val obj: SingleOptionShort = bytes.unpickle[SingleOptionShort]
        assert(obj === new SingleOptionShort(Some(short)))
    }
  }

  test("Round trip a case class with a single optional Short field") {
    forAll {
      (obj: SingleOptionShort) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionShort = pckl.unpickle[SingleOptionShort]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Char
  test("Pickle a case class with a single optional Char field") {
    forAll {
      (obj: SingleOptionChar) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id.map(_.toInt), "/avro/option/SingleOptionChar.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Char field") {
    forAll {
      (char: Char) =>
        val bytes = generateSingleValueBytesFromAvro(Some(char.toInt), "/avro/option/SingleOptionChar.avsc")
        val obj: SingleOptionChar = bytes.unpickle[SingleOptionChar]
        assert(obj === new SingleOptionChar(Some(char)))
    }
  }

  test("Round trip a case class with a single optional Char field") {
    forAll {
      (obj: SingleOptionChar) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOptionChar = pckl.unpickle[SingleOptionChar]
        assert(hydratedObj === obj)
    }
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
