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

  implicit val arbSingleOptionInt = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Int.MinValue, Int.MaxValue)) yield SingleOpInt(opt(exists,num)))
  implicit val arbSingleOptionLong = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Long.MinValue, Long.MaxValue)) yield SingleOpLong(opt(exists,num)))
  implicit val arbSingleOptionDouble = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Double.MinValue/2, Double.MaxValue/2)) yield SingleOpDouble(opt(exists, num)))
  implicit val arbSingleOptionFloat = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Float.MinValue, Float.MaxValue)) yield SingleOpFloat(opt(exists, num)))
  implicit val arbSingleOptionBoolean = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.oneOf(true, false)) yield SingleOpBoolean(opt(exists, num)))
  implicit val arbSingleOptionString = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.alphaStr) yield SingleOpString(opt(exists, num)))
  implicit val arbSingleOptionByte = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Byte.MinValue, Byte.MaxValue)) yield SingleOpByte(opt(exists, num)))
  implicit val arbSingleOptionShort = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Short.MinValue, Short.MaxValue)) yield SingleOpShort(opt(exists, num)))
  implicit val arbSingleOptionChar = Arbitrary(for (exists <- Gen.oneOf(true, false); num <- Gen.choose(Char.MinValue, Char.MaxValue)) yield SingleOpChar(opt(exists, num)))

}

class SingleOptionalFieldTest extends FunSuite with Assertions with GeneratorDrivenPropertyChecks {

  //Single Option Int
  test("Pickle a case class with a single optional int field") {
    forAll {
      (obj: SingleOpInt) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionInt.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional int field") {
    forAll {
      (int: Int) =>
        val bytes = generateSingleValueBytesFromAvro(Some(int), "/avro/option/SingleOptionInt.avsc")
        val obj: SingleOpInt = bytes.unpickle[SingleOpInt]
        assert(obj === new SingleOpInt(Some(int)))
    }
  }

  test("Round trip a case class with a single optional int field") {
    forAll {
      (obj: SingleOpInt) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpInt = pckl.unpickle[SingleOpInt]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Long
  test("Pickle a case class with a single optional long field") {
    forAll {
      (obj: SingleOpLong) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionLong.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional long field") {
    forAll {
      (long: Long) =>
        val bytes = generateSingleValueBytesFromAvro(Some(long), "/avro/option/SingleOptionLong.avsc")
        val obj: SingleOpLong = bytes.unpickle[SingleOpLong]
        assert(obj === new SingleOpLong(Some(long)))
    }
  }

  test("Round trip a case class with a single optional long field") {
    forAll {
      (obj: SingleOpLong) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpLong = pckl.unpickle[SingleOpLong]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Double
  test("Pickle a case class with a single optional double field") {
    forAll {
      (obj: SingleOpDouble) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionDouble.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional double field") {
    forAll {
      (double: Double) =>
        val bytes = generateSingleValueBytesFromAvro(Some(double), "/avro/option/SingleOptionDouble.avsc")
        val obj: SingleOpDouble = bytes.unpickle[SingleOpDouble]
        assert(obj === new SingleOpDouble(Some(double)))
    }
  }

  test("Round trip a case class with a single optional double field") {
    forAll {
      (obj: SingleOpDouble) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpDouble = pckl.unpickle[SingleOpDouble]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Float
  test("Pickle a case class with a single optional Float field") {
    forAll {
      (obj: SingleOpFloat) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionFloat.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Float field") {
    forAll {
      (float: Float) =>
        val bytes = generateSingleValueBytesFromAvro(Some(float), "/avro/option/SingleOptionFloat.avsc")
        val obj: SingleOpFloat = bytes.unpickle[SingleOpFloat]
        assert(obj === new SingleOpFloat(Some(float)))
    }
  }

  test("Round trip a case class with a single optional Float field") {
    forAll {
      (obj: SingleOpFloat) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpFloat = pckl.unpickle[SingleOpFloat]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Boolean
  test("Pickle a case class with a single optional Boolean field") {
    forAll {
      (obj: SingleOpBoolean) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionBoolean.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Boolean field") {
    forAll {
      (boolean: Boolean) =>
        val bytes = generateSingleValueBytesFromAvro(Some(boolean), "/avro/option/SingleOptionBoolean.avsc")
        val obj: SingleOpBoolean = bytes.unpickle[SingleOpBoolean]
        assert(obj === new SingleOpBoolean(Some(boolean)))
    }
  }

  test("Round trip a case class with a single optional Boolean field") {
    forAll {
      (obj: SingleOpBoolean) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpBoolean = pckl.unpickle[SingleOpBoolean]
        assert(hydratedObj === obj)
    }
  }

  //Single Option String
  test("Pickle a case class with a single optional String field") {
    forAll {
      (obj: SingleOpString) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id, "/avro/option/SingleOptionString.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional String field") {
    forAll {
      (string: String) =>
        val bytes = generateSingleValueBytesFromAvro(Some(string), "/avro/option/SingleOptionString.avsc")
        val obj: SingleOpString = bytes.unpickle[SingleOpString]
        assert(obj === new SingleOpString(Some(string)))
    }
  }

  test("Round trip a case class with a single optional String field") {
    forAll {
      (obj: SingleOpString) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpString = pckl.unpickle[SingleOpString]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Byte
  test("Pickle a case class with a single optional Byte field") {
    forAll {
      (obj: SingleOpByte) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id.map(_.toInt), "/avro/option/SingleOptionByte.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Byte field") {
    forAll {
      (byte: Byte) =>
        val bytes = generateSingleValueBytesFromAvro(Some(byte.toInt), "/avro/option/SingleOptionByte.avsc")
        val obj: SingleOpByte = bytes.unpickle[SingleOpByte]
        assert(obj === new SingleOpByte(Some(byte)))
    }
  }

  test("Round trip a case class with a single optional Byte field") {
    forAll {
      (obj: SingleOpByte) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpByte = pckl.unpickle[SingleOpByte]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Short
  test("Pickle a case class with a single optional Short field") {
    forAll {
      (obj: SingleOpShort) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id.map(_.toInt), "/avro/option/SingleOptionShort.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Short field") {
    forAll {
      (short: Short) =>
        val bytes = generateSingleValueBytesFromAvro(Some(short.toInt), "/avro/option/SingleOptionShort.avsc")
        val obj: SingleOpShort = bytes.unpickle[SingleOpShort]
        assert(obj === new SingleOpShort(Some(short)))
    }
  }

  test("Round trip a case class with a single optional Short field") {
    forAll {
      (obj: SingleOpShort) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpShort = pckl.unpickle[SingleOpShort]
        assert(hydratedObj === obj)
    }
  }

  //Single Option Char
  test("Pickle a case class with a single optional Char field") {
    forAll {
      (obj: SingleOpChar) =>
        val pckl = obj.pickle
        assert(generateSingleValueBytesFromAvro(obj.id.map(_.toInt), "/avro/option/SingleOptionChar.avsc") === pckl.value)
    }
  }

  test("Unpickle a case class with a single optional Char field") {
    forAll {
      (char: Char) =>
        val bytes = generateSingleValueBytesFromAvro(Some(char.toInt), "/avro/option/SingleOptionChar.avsc")
        val obj: SingleOpChar = bytes.unpickle[SingleOpChar]
        assert(obj === new SingleOpChar(Some(char)))
    }
  }

  test("Round trip a case class with a single optional Char field") {
    forAll {
      (obj: SingleOpChar) =>
        val pckl = obj.pickle
        val hydratedObj: SingleOpChar = pckl.unpickle[SingleOpChar]
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
